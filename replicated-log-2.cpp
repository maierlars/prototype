#include "replicated-log-2.h"

#include <velocypack/Iterator.h>

#include <velocypack/velocypack-aliases.h>

void rlog2::LogEntry::intoBuilder(VPackBuilder& builder) const {
  VPackObjectBuilder ob(&builder);
  builder.add("index", VPackValue(index));
  builder.add("term", VPackValue(term));
  builder.add("payload", VPackSlice(payload.data()));
}

rlog2::LogEntry::LogEntry(VPackSlice slice) {
  index = slice.get("index").getNumber<log_index_type>();
  term = slice.get("term").getNumber<log_term_type>();
  auto payloadSlice = slice.get("payload");
  payload.append(payloadSlice.begin(), payloadSlice.byteSize());
}

rlog2::LogEntry::LogEntry(rlog2::log_index_type index,
                          rlog2::log_term_type term, VPackBufferUInt8 payload)
    : index(index), term(term), payload(std::move(payload)) {
  assert(index != 0);
}

rlog2::AppendEntriesRequest::AppendEntriesRequest(rlog2::log_term_type term,
                                                  rlog2::log_index_type prevLogIndex,
                                                  rlog2::log_term_type prevLogTerm,
                                                  rlog2::log_index_type commitIndex,
                                                  std::unique_ptr<LogIterator> entries)
    : term(term),
      prevLogIndex(prevLogIndex),
      prevLogTerm(prevLogTerm),
      commitIndex(commitIndex),
      entries(std::move(entries)) {}

void rlog2::AppendEntriesRequest::intoBuilder(VPackBuilder& builder) const {
  VPackObjectBuilder ob(&builder);
  builder.add("term", VPackValue(term));
  builder.add("prevLogIndex", VPackValue(prevLogIndex));
  builder.add("prevLogTerm", VPackValue(prevLogTerm));
  builder.add("commitIndex", VPackValue(commitIndex));

  {
    VPackArrayBuilder ab(&builder, "entries");
    while (auto e = entries->next()) {
      e->intoBuilder(builder);
    }
  }
}

rlog2::AppendEntriesRequest::AppendEntriesRequest(VPackSlice slice) {
  term = slice.get("term").getNumber<log_index_type>();
  prevLogIndex = slice.get("prevLogIndex").getNumber<log_index_type>();
  prevLogTerm = slice.get("prevLogTerm").getNumber<log_index_type>();
  commitIndex = slice.get("commitIndex").getNumber<log_index_type>();

  using container = std::vector<LogEntry>;

  auto entriesSlice = slice.get("entries");

  container v;
  v.reserve(entriesSlice.length());

  for (auto const& e : VPackArrayIterator(slice.get("entries"))) {
    v.emplace_back(e);
  }

  struct Iterator final : LogIterator {
    auto next() -> std::optional<LogEntry> override {
      if (iter == v.end()) {
        return std::nullopt;
      } else {
        return *(iter++);
      }
    }

    void reset() override { iter = v.begin(); }

    explicit Iterator(container vp) : v(std::move(vp)), iter(v.begin()) {}

    container v;
    container::iterator iter;
  };

  entries = std::make_unique<Iterator>(std::move(v));
}

rlog2::AppendEntriesResult::AppendEntriesResult(VPackSlice slice) {
  term = slice.get("term").getNumber<log_term_type>();
  success = slice.get("success").isTrue();
}

void rlog2::AppendEntriesResult::intoBuilder(VPackBuilder& builder) const {
  VPackObjectBuilder ob(&builder);
  builder.add("term", VPackValue(term));
  builder.add("success", VPackValue(success));
}

rlog2::AppendEntriesResult::AppendEntriesResult(rlog2::log_term_type term, bool success)
    : term(term), success(success) {}

rlog2::ReplicatedLog::ReplicatedLog(rlog2::log_id_type logId_,
                                    std::shared_ptr<RocksDBHandle> rocks_,
                                    std::shared_ptr<sched::scheduler> scheduler_)
    : _logId(logId_), rocks(std::move(rocks_)), scheduler(std::move(scheduler_)) {
  rocksdb::ReadOptions opts;
  auto iter = this->rocks->db->NewIterator(opts, this->rocks->logs.get());

  std::string prefix;
  insert_big_endian_uint64(prefix, this->_logId + 1);
  iter->Seek(rocksdb::Slice(prefix));

  if (auto status = iter->status(); !status.ok()) {
    throw RocksDBException(status);
  }

  while (iter->Valid() && iter->key().starts_with(rocksdb::Slice(prefix))) {
    _log.emplace_back(VPackSlice(reinterpret_cast<const uint8_t*>(iter->key().data())));
    if (_log.size() == 1000) {
      break;
    }

    iter->Prev();
  }

  if (!_log.empty()) {
    firstMemoryIndex = _log.front().index;
    currentIndex = _log.back().index;
    currentTerm = _log.back().term;
    storeIndex = _log.back().index;
  }
}

auto rlog2::ReplicatedLog::insert(VPackBufferUInt8 payload)
    -> std::pair<log_index_type, log_term_type> {
  std::unique_lock guard(mutex);

  if (state != State::LEADER) {
    throw std::logic_error("not a leader");
  }

  _log.emplace_back(currentIndex + 1, currentTerm, std::move(payload));
  currentIndex += 1;

  triggerStore();
  triggerReplication();
  return {currentIndex, currentTerm};
}

auto rlog2::ReplicatedLog::waitFor(rlog2::log_index_type index)
    -> futures::future<WaitResult> {
  std::unique_lock guard(mutex);
  if (state != State::LEADER) {
    throw std::logic_error("not a leader");
  }

  if (index < firstMemoryIndex) {
    throw std::runtime_error("index no longer in memory");
  }

  auto relativeIndex = index - firstMemoryIndex;
  if (relativeIndex >= _log.size()) {
    throw std::logic_error("unknown log index");
  }

  if (commitIndex >= index) {
    return futures::make_fulfilled_promise<WaitResult>(std::in_place, _log.operator[](relativeIndex).term);
  }

  auto&& [f, p] = futures::make_promise<WaitResult>();
  notify_queue.emplace(index, std::move(p));

  return std::move(f);
}

futures::future<void> rlog2::ReplicatedLog::assumeLeadership(rlog2::log_term_type term) {
  std::unique_lock guard(mutex);
  if (state != State::FOLLOWER) {
    throw std::logic_error("not a follower");
  }
  if (term < currentTerm) {
    throw std::logic_error("invalid term");
  }

  state = State::TRANSITION;
  // wait for all store operations to complete
  return waitForAllTasks().then([this, self = shared_from_this(), term](auto&& e) {
    std::unique_lock guard(mutex);
    if (e.has_error()) {
      // failed to do something
      state = State::FOLLOWER;
      abort();
    } else {
      state = State::LEADER;
      currentTerm = term;
    }
  });
}

futures::future<void> rlog2::ReplicatedLog::waitForAllTasks() {
  std::vector<futures::future<void>> tasks;
  tasks.emplace_back(std::move(storeOperation));

  return futures::fan_in(tasks.begin(), tasks.end()).then([](auto&& e) {
    e.rethrow_error();
  });
}

futures::future<void> rlog2::ReplicatedLog::resignLeadership() {
  std::unique_lock guard(mutex);
  if (state != State::LEADER) {
    throw std::logic_error("not a leader");
  }

  // wait for store and replication to finish
  state = State::TRANSITION;
  // wait for all store operations to complete
  return waitForAllTasks().then([this, self = shared_from_this()](auto&& e) {
    std::unique_lock guard(mutex);
    if (e.has_error()) {
      // failed to do something
      state = State::LEADER;
      abort();
    } else {
      state = State::FOLLOWER;
    }
  });
}

auto rlog2::ReplicatedLog::appendEntries(rlog2::AppendEntriesRequest const& req)
    -> futures::future<AppendEntriesResult> {
  std::unique_lock guard(mutex);

  if (state == State::FOLLOWER && req.term >= currentTerm) {
    // update my term
    currentTerm = req.term;
    commitIndex = req.commitIndex;

    // check log end
    if (req.prevLogIndex <= currentIndex) {
      if (req.prevLogIndex < firstMemoryIndex) {
        throw std::runtime_error("entry not in memory");
      }

      auto relativeIndex = req.prevLogIndex - firstMemoryIndex;

      auto& entry = _log.at(relativeIndex);
      if (entry.term == req.prevLogTerm) {
        // delete all entries following entry
        _log.erase(_log.begin() + relativeIndex, _log.end());
        // insert the new entries
        while (auto e = req.entries->next()) {
          _log.emplace_back(std::move(e).value());
        }
        storeIndex = req.prevLogIndex;

        return runStoreOperation()
            .and_then([term = currentTerm] {
              std::cout << "appended entries" << std::endl;
              return AppendEntriesResult{term, true};
            })
            .throw_nested<std::logic_error>("failed to append entries");
      } else {
        std::cout << "term of prevlog index does not match " << entry.term
                  << " != " << req.prevLogTerm << std::endl;
      }
    } else {
      std::cout << "prevlog index is after my log head" << _log.size() << std::endl;
    }
  } else {
    std::cout << "my term is higher " << currentTerm << std::endl;
  }

  std::cout << "returning false to replication " << std::endl;
  return futures::make_fulfilled_promise<AppendEntriesResult>(std::in_place, currentTerm, false);
}
