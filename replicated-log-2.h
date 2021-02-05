#ifndef PROTOTYPE_REPLICATED_LOG_2_H
#define PROTOTYPE_REPLICATED_LOG_2_H
#include <iostream>
#include <memory>
#include <mutex>

#include <velocypack/Buffer.h>
#include <velocypack/Builder.h>

#include <velocypack/velocypack-aliases.h>

#include "deserialize.h"
#include "futures.h"
#include "rocksdb-handle.h"
#include "rocksdb-transaction.h"
#include "scheduler.h"

namespace rlog2 {

using log_index_type = uint64_t;
using log_term_type = uint64_t;
using log_id_type = uint64_t;

constexpr const auto invalid_log_index = std::numeric_limits<log_index_type>::max();

struct WaitResult {
  log_term_type term;
  explicit WaitResult(log_term_type term_) : term(term_) {}
};

struct LogEntry {
  log_index_type index;
  log_term_type term;
  VPackBufferUInt8 payload;

  LogEntry(log_index_type index, log_term_type term, VPackBufferUInt8 payload);

  void intoBuilder(VPackBuilder& builder) const;
  explicit LogEntry(VPackSlice slice);
};

template <typename T>
struct Iterator {
  virtual ~Iterator() = default;
  virtual auto next() -> std::optional<T> = 0;
  virtual void reset() = 0;
};

using LogIterator = Iterator<LogEntry>;

struct AppendEntriesResult {
  log_term_type term;
  bool success;

  AppendEntriesResult(log_term_type term, bool success);

  explicit AppendEntriesResult(VPackSlice slice);
  void intoBuilder(VPackBuilder& builder) const;
};

struct AppendEntriesRequest {
  log_term_type term;
  log_index_type prevLogIndex;
  log_term_type prevLogTerm;
  log_index_type commitIndex;

  std::unique_ptr<LogIterator> entries;

  AppendEntriesRequest(log_term_type term, log_index_type prevLogIndex,
                       log_term_type prevLogTerm, log_index_type commitIndex,
                       std::unique_ptr<LogIterator> entries);

  void intoBuilder(VPackBuilder& builder) const;
  explicit AppendEntriesRequest(VPackSlice slice);
};

namespace {

void insert_big_endian_uint64(std::string& str, uint64_t v) {
  str.push_back((v >> 56) & 0xff);
  str.push_back((v >> 48) & 0xff);
  str.push_back((v >> 40) & 0xff);
  str.push_back((v >> 32) & 0xff);
  str.push_back((v >> 24) & 0xff);
  str.push_back((v >> 16) & 0xff);
  str.push_back((v >> 8) & 0xff);
  str.push_back(v & 0xff);
}

}  // namespace

struct ReplicatedLog : std::enable_shared_from_this<ReplicatedLog> {
  ReplicatedLog(log_id_type logId_, std::shared_ptr<RocksDBHandle> rocks_,
                std::shared_ptr<sched::scheduler> scheduler_);

  auto insert(VPackBufferUInt8 payload) -> std::pair<log_index_type, log_term_type>;

  auto waitFor(log_index_type index) -> futures::future<WaitResult>;

  auto appendEntries(AppendEntriesRequest const& req)
      -> futures::future<AppendEntriesResult>;

  futures::future<void> assumeLeadership(log_term_type term);

  futures::future<void> resignLeadership();

 private:
  futures::future<void> waitForAllTasks();

  struct NotifyRequest {
    NotifyRequest(log_index_type index_, futures::promise<WaitResult> promise_)
        : index(index_), promise(std::move(promise_)) {}
    log_index_type index;
    futures::promise<WaitResult> promise;

    bool operator<(NotifyRequest const& other) const {
      return other.index < index;
    }
  };
  foobar::priority_queue<NotifyRequest> notify_queue;

 private:
  bool storeOperationOngoing = false;
  futures::future<void> storeOperation = futures::make_fulfilled_promise();

  void flushLogToRocksDB() {
    rocksdb::WriteBatch batch;
    log_index_type lastIndex;
    {
      std::unique_lock guard(mutex);
      lastIndex = currentIndex;
      if (storeIndex == currentIndex) {
        return;
      }

      for (auto idx = storeIndex + 1; idx <= lastIndex; idx++) {
        LogEntry const& entry = _log[idx - firstMemoryIndex];
        VPackBuilder data;
        entry.intoBuilder(data);

        std::string key;
        insert_big_endian_uint64(key, _logId);
        insert_big_endian_uint64(key, entry.index);

        batch.Put(rocks->logs.get(), rocksdb::Slice(key),
                  rocksdb::Slice(reinterpret_cast<const char*>(data.data()), data.size()));
      }
    }

    rocksdb::WriteOptions opts;
    opts.sync = true;
    auto status = rocks->db->Write(opts, &batch);
    if (!status.ok()) {
      throw RocksDBException(status);
    }

    {
      std::unique_lock guard(mutex);
      storeIndex = lastIndex;
    }
  }

  futures::future<void> runStoreOperation() {
    return scheduler->async([this] { flushLogToRocksDB(); })
        .and_then([this] {
          std::unique_lock guard(mutex);

          if (currentIndex > storeIndex) {
            // run another store operation
            return runStoreOperation();
          }
          return futures::make_fulfilled_promise();
        })
        .then([this, self = shared_from_this()](expect::expected<void>&& e) noexcept {
          try {
            e.rethrow_error();
          } catch (...) {
            std::cout << "exception in store thread";
            abort();  // todo
          }

          storeOperationOngoing = false;
        });
  }

  void triggerStore() {
    if (!storeOperationOngoing) {
      storeOperationOngoing = true;
      storeOperation = runStoreOperation();
    }
  }

  void triggerReplication();

  std::mutex mutex;

  log_index_type currentIndex = 0;
  log_term_type currentTerm = 0;
  log_index_type commitIndex = 0;
  log_index_type firstMemoryIndex = 1;
  log_index_type storeIndex = 0;

  log_id_type _logId;

  enum class State { FOLLOWER, LEADER, TRANSITION };

  State state = State::FOLLOWER;

  std::deque<LogEntry> _log;

  std::shared_ptr<RocksDBHandle> rocks;
  std::shared_ptr<sched::scheduler> scheduler;
};
}  // namespace rlog2

#endif  // PROTOTYPE_REPLICATED_LOG_2_H
