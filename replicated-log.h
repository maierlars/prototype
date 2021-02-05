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
#include <iostream>

#include "futures.h"
#include "priority-queue.h"
#include "scheduler.h"

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

  void intoBuilder(VPackBuilder& builder) const {
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
  explicit Participant(std::shared_ptr<ParticipantAPI> api)
      : api(std::move(api)) {}

  std::shared_ptr<ParticipantAPI> api;
  log_index_type index = 0;
  bool replicationOngoing = false;
};

struct ReplicatedLogReplicatorAPI {
  virtual ~ReplicatedLogReplicatorAPI() = default;
  virtual auto getCommitIndex() -> log_index_type = 0;
  virtual void updateCommitIndex(log_index_type) = 0;
  virtual auto getLogEntry(log_index_type) -> LogEntry = 0;
  virtual auto getLogFrom(log_index_type)
      -> std::pair<immer::box<LogEntry>, std::unique_ptr<LogIterator>> = 0;
};

struct ReplicatedLog final : ReplicatedLogTrxAPI, ReplicatedLogLeadershipAPI, ReplicatedLogReplicationAPI {
  explicit ReplicatedLog(std::vector<std::shared_ptr<ParticipantAPI>> const& parts,
                         std::shared_ptr<sched::scheduler> scheduler)
      : scheduler(std::move(scheduler)) {
    std::transform(parts.begin(), parts.end(), std::back_inserter(participants),
                   [](std::shared_ptr<ParticipantAPI> const& api) {
                     return Participant{api};
                   });
  }

  auto insert(VPackBufferUInt8 payload) -> std::pair<log_index_type, log_term_type> override {
    std::unique_lock guard(_mutex);
    if (!is_leader) {
      throw std::logic_error("not a leader");
    }

    std::cout << "Inserting " << currentIndex + 1 << " " << currentTerm << " "
              << VPackSlice(payload.data()).toJson() << std::endl;

    _log = std::move(_log).push_back({currentIndex + 1, currentTerm, std::move(payload)});
    currentIndex += 1;

    triggerReplication();
    return std::make_pair(currentIndex, currentTerm);
  }

  auto waitFor(log_index_type index, log_term_type term)
      -> futures::future<WaitResult> override {
    std::unique_lock guard(_mutex);
    if (!is_leader) {
      throw std::logic_error("not a leader");
    }

    std::cout << "waiting for " << index << " " << term << std::endl;

    return std::invoke([&] {
             if (index <= commitIndex) {
               std::cout << "wait condition already satsified" << std::endl;
               return futures::make_fulfilled_promise<WaitResult>(
                   std::in_place, _log[index - 1]->term);
             }

             auto&& [f, p] = futures::make_promise<WaitResult>();
             notifications.emplace(index, term, std::move(p));
             return std::move(f);
           })
        .then_value([term, index](WaitResult&& result) {
          std::cout << "waiting completed " << index << " " << term
                    << " with actual term " << result.term << std::endl;
          if (result.term != term) {
            throw std::logic_error("index has wrong term");
          }
          return result;
        });
  }

  void assumeLeadership(log_term_type term) override {
    std::unique_lock guard(_mutex);
    std::cout << "Assuming leadership for term " << term << std::endl;
    is_leader = true;
    currentTerm = term;
  }

  auto appendEntries(const AppendEntriesRequest& req) -> AppendEntriesResult override {
    std::unique_lock guard(_mutex);
    std::cout << "received append entries with term " << req.term << " prevLog = ("
              << req.prevLogIndex << " " << req.prevLogTerm << std::endl;
    if (!is_leader && req.term >= currentTerm) {
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
          std::cout << "appended entries" << std::endl;
          return {currentTerm, true};
        } else {
          std::cout << "term of prevlog index does not match " << entry->term << " != " << req.prevLogTerm << std::endl;
        }
      } else {
        std::cout << "prevlog index is after my log head" << _log.size() << std::endl;
      }
    } else {
      std::cout << "my term is higher " << currentTerm << std::endl;
    }

    std::cout << "returning false to replication " << std::endl;
    return {currentTerm, false};
  }

 private:
  log_index_type quorumIndex(std::size_t quorum_size) const {
    quorum_size = std::max(1ul, std::min(quorum_size, participants.size()));

    std::vector<log_index_type> indexes;
    std::transform(participants.begin(), participants.end(), std::back_inserter(indexes),
                   [](Participant const& p) { return p.index; });
    std::nth_element(indexes.begin(), indexes.begin() + (quorum_size - 1),
                     indexes.end(), std::greater<log_index_type>{});
    return indexes.at(quorum_size - 1);
  }

  void updateCommitIndex(log_index_type index) {
    assert(index > commitIndex);
    currentIndex = index;
    while (!notifications.empty()) {
      if (notifications.find_max().index <= index) {
        auto&& top = notifications.extract_max();
        std::move(top.promise).fulfill(std::in_place, _log[index - 1]->term);
      } else {
        break;
      }
    }
  }

  void checkCommitIndex() {
    auto index = quorumIndex(participants.size() / 2 + 1);
    if (index > commitIndex) {
      std::cout << "increment commit index to " << index << std::endl;
      updateCommitIndex(index);
    }
  }

  void triggerReplication() noexcept {
    for (auto&& p : participants) {
      if (!p.replicationOngoing && p.index < currentIndex) {
        startReplication(p);
      }
    }
  }

  futures::future<void> sendReplication(Participant& p) {
    return futures::capture_into_future([&] {
             std::unique_lock guard(_mutex);
             auto const& prevLog = _log[p.index];

             auto log = _log.drop(p.index);

             auto req = AppendEntriesRequest(currentTerm, prevLog->index,
                                             prevLog->term, commitIndex,
                                             std::make_unique<Iterator>(log));

             return p.api->appendEntries(req).then_value([log](rlog::AppendEntriesResult&& result) {
               return std::make_tuple(result, log.size());
             });
           })
        .then_value([this, &p](std::tuple<rlog::AppendEntriesResult, std::size_t>&& tup) {
          std::unique_lock guard(_mutex);
          auto&& [result, length] = tup;
          if (result.success) {
            p.index += length;
            std::cout << "Participant is now at " << p.index << std::endl;
            checkCommitIndex();
            if (p.index < currentIndex) {
              return sendReplication(p);
            }
          } else if (p.index > 0) {
            p.index -= 1;
          } else {
            throw std::logic_error("follower does not accept any entries");
          }

          return futures::make_fulfilled_promise();
        });
  }

  void startReplication(Participant& p) {
    p.replicationOngoing = true;
    scheduler
        ->async([&]() -> futures::future<void> { return sendReplication(p); })
        .finally([&](expect::expected<void>&& e) noexcept {
          std::unique_lock guard(_mutex);
          p.replicationOngoing = false;
          std::cout << "stopping replication for one participant" << std::endl;
          try {
            e.rethrow_error();
          } catch (std::exception const& ex) {
            std::cout << "replication request ended with exception: " << ex.what()
                      << std::endl;
          } catch (...) {
            std::cout << "replication request ended with unknown exception" << std::endl;
          }
        });
  }

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

  static_assert(std::is_nothrow_move_assignable_v<NotifyRequest>);

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

  std::mutex mutable _mutex;
  foobar::priority_queue<NotifyRequest> notifications;
  immer::flex_vector<immer::box<LogEntry>> _log;
  std::vector<Participant> participants;

  log_index_type currentIndex = 0;
  log_term_type currentTerm = 0;
  log_index_type commitIndex = 0;
  bool is_leader = false;
  std::shared_ptr<sched::scheduler> scheduler;
};

}  // namespace rlog
#endif  // PROTOTYPE_REPLICATED_LOG_H
