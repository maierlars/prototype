#ifndef PROTOTYPE_REPLICATED_LOG_H
#define PROTOTYPE_REPLICATED_LOG_H
#include <condition_variable>
#include <mutex>
#include <thread>

#include <immer/box.hpp>
#include <immer/flex_vector.hpp>
#include <immer/flex_vector_transient.hpp>

#include <velocypack/Buffer.h>
#include <velocypack/Builder.h>
#include <velocypack/Iterator.h>
#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

#include "futures.h"
#include "priority-queue.h"

namespace rlog {

using log_index_type = std::size_t;
using log_term_type = std::size_t;

struct ReplicatedLogTrxAPI {
  virtual ~ReplicatedLogTrxAPI() = default;

  virtual auto insert(VPackBufferUInt8 payload)
      -> std::pair<log_index_type, log_term_type> = 0;

  struct WaitResult {
    log_term_type term;

    explicit WaitResult(log_term_type term) : term(term) {}
  };

  auto waitFor(std::pair<log_index_type, log_term_type> p) -> futures::future<WaitResult> {
    auto [index, term] = p;
    return waitFor(index, term);
  }

  virtual auto waitFor(log_index_type, log_term_type) -> futures::future<WaitResult> = 0;
};

struct ReplicatedLogLeadershipAPI {
  virtual ~ReplicatedLogLeadershipAPI() = default;
  virtual void assumeLeadership(log_term_type) = 0;
};

struct LogEntry {
  log_index_type index;
  log_term_type term;
  VPackBufferUInt8 payload;

  void intoBuilder(VPackBuilder& builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add("index", VPackValue(index));
    builder.add("term", VPackValue(term));
    builder.add("payload", VPackSlice(payload.data()));
  }

  explicit LogEntry(VPackSlice slice) {
    index = slice.get("index").getNumber<log_index_type>();
    term = slice.get("term").getNumber<log_term_type>();
    auto payloadSlice = slice.get("payload");
    payload.append(payloadSlice.begin(), payloadSlice.byteSize());
  }

  LogEntry(log_index_type index, log_term_type term, VPackBufferUInt8 payload)
      : index(index), term(term), payload(std::move(payload)) {}
};

struct LogIterator {
  virtual ~LogIterator() = default;
  virtual auto next() -> std::optional<immer::box<LogEntry>> = 0;
};

/* seite 4 */
struct AppendEntriesResult {
  log_term_type term;
  bool success;

  explicit AppendEntriesResult(VPackSlice slice) {
    term = slice.get("term").getNumber<log_term_type>();
    success = slice.get("success").isTrue();
  }

  void intoBuilder(VPackBuilder &builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add("term", VPackValue(term));
    builder.add("success", VPackValue(success));
  }

  AppendEntriesResult(log_term_type term, bool success)
      : term(term), success(success) {}
};

struct AppendEntriesRequest {
  log_term_type term;
  log_index_type prevLogIndex;
  log_term_type prevLogTerm;
  log_index_type commitIndex;

  std::unique_ptr<LogIterator> entries;

  AppendEntriesRequest(log_term_type term, log_index_type prevLogIndex,
                       log_term_type prevLogTerm, log_index_type commitIndex,
                       std::unique_ptr<LogIterator> entries)
      : term(term),
        prevLogIndex(prevLogIndex),
        prevLogTerm(prevLogTerm),
        commitIndex(commitIndex),
        entries(std::move(entries)) {}

  void intoBuilder(VPackBuilder& builder) const {
    VPackObjectBuilder ob(&builder);
    builder.add("term", VPackValue(term));
    builder.add("prevLogIndex", VPackValue(prevLogIndex));
    builder.add("prevLogTerm", VPackValue(prevLogTerm));
    builder.add("commitIndex", VPackValue(commitIndex));

    {
      VPackArrayBuilder ab(&builder, "entries");
      while (auto e = entries->next()) {
        e->get().intoBuilder(builder);
      }
    }
  }

  explicit AppendEntriesRequest(VPackSlice slice) {
    term = slice.get("term").getNumber<log_index_type>();
    prevLogIndex = slice.get("prevLogIndex").getNumber<log_index_type>();
    prevLogTerm = slice.get("prevLogTerm").getNumber<log_index_type>();
    commitIndex = slice.get("commitIndex").getNumber<log_index_type>();

    using container = std::vector<immer::box<LogEntry>>;

    auto entriesSlice = slice.get("entries");
    container v;
    v.reserve(entriesSlice.length());

    for (auto const& e : VPackArrayIterator(slice.get("entries"))) {
      v.emplace_back(e);
    }

    struct Iterator final : LogIterator {
      auto next() -> std::optional<immer::box<LogEntry>> override {
        if (iter == v.end()) {
          return std::nullopt;
        } else {
          return *(iter++);
        }
      }

      explicit Iterator(container vp) : v(std::move(vp)), iter(v.begin()) {}

      container v;
      container::iterator iter;
    };

    entries = std::make_unique<Iterator>(std::move(v));
  }
};

struct ReplicatedLogReplicationAPI {
  virtual ~ReplicatedLogReplicationAPI() = default;
  virtual auto appendEntries(AppendEntriesRequest const&) -> AppendEntriesResult = 0;
};

struct ParticipantAPI {
  virtual ~ParticipantAPI() = default;
  virtual auto appendEntries(AppendEntriesRequest const&)
      -> futures::future<AppendEntriesResult> = 0;
};

struct Participant {
  std::shared_ptr<ParticipantAPI> api;
  log_index_type index;
};

struct ReplicatedLogReplicatorAPI {
  virtual ~ReplicatedLogReplicatorAPI() = default;
  virtual auto getCommitIndex() -> log_index_type = 0;
  virtual void updateCommitIndex(log_index_type) = 0;
  virtual auto getLogEntry(log_index_type) -> LogEntry = 0;
  virtual auto getLogFrom(log_index_type) -> std::pair<immer::box<LogEntry>, std::unique_ptr<LogIterator>> = 0;
};

struct ReplicatedLog final : ReplicatedLogTrxAPI,
                             ReplicatedLogLeadershipAPI,
                             ReplicatedLogReplicationAPI,
                             ReplicatedLogReplicatorAPI {
  auto insert(VPackBufferUInt8 payload) -> std::pair<log_index_type, log_term_type> override {
    std::unique_lock guard(_mutex);
    if (!is_leader) {
      throw std::logic_error("not a leader");
    }

    _log = std::move(_log).push_back({currentIndex + 1, currentTerm, std::move(payload)});
    currentIndex += 1;
    return std::make_pair(currentIndex, currentTerm);
  }

  auto waitFor(log_index_type index, log_term_type term)
      -> futures::future<WaitResult> override {
    if (!is_leader) {
      throw std::logic_error("not a leader");
    }

    auto&& [f, p] = futures::make_promise<WaitResult>();
    notifications.emplace(index, term, std::move(p));
    return std::move(f).then_value([term](WaitResult&& result) {
      if (result.term != term) {
        throw std::logic_error("index has wrong term");
      }
      return result;
    });
  }

  void assumeLeadership(log_term_type term) override {
    std::unique_lock guard(_mutex);
    is_leader = true;
    currentTerm = term;
  }

  auto appendEntries(const AppendEntriesRequest& req) -> AppendEntriesResult override {
    std::unique_lock guard(_mutex);
    if (is_leader && req.term >= currentTerm) {
      // update my term
      currentTerm = req.term;
      commitIndex = req.commitIndex;

      // check log end
      if (req.prevLogIndex < _log.size()) {
        auto& entry = _log.operator[](req.prevLogIndex);
        if (entry->term == req.prevLogTerm) {
          // append to the log
          auto t = _log.transient();
          t.take(req.prevLogIndex);
          while (auto opt = req.entries->next()) {
            t.push_back(*opt);
          }
          _log = t.persistent();
          return {currentTerm, true};
        }
      }
    }

    return {currentTerm, false};
  }

  void updateCommitIndex(log_index_type index) override {
    std::unique_lock guard(_mutex);
    assert(index > currentIndex);
    currentIndex = index;
    while (!notifications.empty()) {
      if (notifications.find_max().index <= index) {
        auto&& top = notifications.extract_max();
        std::move(top.promise).fulfill(std::in_place, _log[index]->term);
        abort();
      } else {
        break;
      }
    }
  }

  auto getLogFrom(log_index_type index) -> std::pair<immer::box<LogEntry>, std::unique_ptr<LogIterator>> override {
    std::unique_lock guard(_mutex);
    auto prevLogEntry = _log.at(index);
    auto t = _log.transient();
    t.drop(index);
    t.take(500);
    return {std::move(prevLogEntry), std::make_unique<Iterator>(t.persistent())};
  }

 private:
  struct NotifyRequest {
    NotifyRequest(log_index_type index_, log_term_type term_,
                  futures::promise<ReplicatedLogTrxAPI::WaitResult> promise_)
        : index(index_), term(term_), promise(std::move(promise_)) {}
    log_index_type index;
    log_term_type term;
    futures::promise<ReplicatedLogTrxAPI::WaitResult> promise;

    bool operator<(NotifyRequest const& other) const {
      return other.index < index;
    }
  };

  struct Iterator : LogIterator {
    using vector_type = immer::flex_vector<immer::box<LogEntry>>;

    explicit Iterator(vector_type v) : log(std::move(v)), iter(log.begin()) {}

    auto next() -> std::optional<immer::box<LogEntry>> override {
      if (iter == log.end()) {
        return std::nullopt;
      } else {
        return *(iter++);
      }
    }

   private:
    vector_type log;
    vector_type::iterator iter;
  };

  std::mutex _mutex;
  foobar::priority_queue<NotifyRequest> notifications;
  immer::flex_vector<immer::box<LogEntry>> _log;

  log_index_type currentIndex = 0;
  log_term_type currentTerm = 0;
  log_index_type commitIndex = 0;
  bool is_leader = false;
};

struct ReplicationThread {
  explicit ReplicationThread(std::shared_ptr<ReplicatedLogReplicatorAPI> log,
                             std::vector<std::shared_ptr<Participant>> parts,
                             log_term_type currentTerm)
      : log(std::move(log)), participants(std::move(parts)), currentTerm(currentTerm) {}

  void stop() {
    {
      std::unique_lock guard(mutex);
      is_stopping = true;
      cv.notify_one();
    }
    thread.join();
  }

  void start() {
    thread = std::thread([this] { this->run(); });
  }

 private:
  void run() noexcept {
    std::unique_lock guard(mutex);
    while (!is_stopping) {
      for (auto&& participant : participants) {
        auto&& [prevLogEntry, iterator] = log->getLogFrom(participant->index);

        auto req = AppendEntriesRequest{
            currentTerm,
            participant->index,
            prevLogEntry->term,
            log->getCommitIndex(),
            std::move(iterator)
        };
        participant->api->appendEntries(req);
      }


      cv.wait(guard);
    }
  }

  bool is_stopping = false;

  std::thread thread;
  std::mutex mutex;
  std::condition_variable cv;
  std::shared_ptr<ReplicatedLogReplicatorAPI> log;
  std::vector<std::shared_ptr<Participant>> participants;
  log_term_type currentTerm;
};
}  // namespace rlog
#endif  // PROTOTYPE_REPLICATED_LOG_H
