#ifndef PROTOTYPE_PERSISTED_LOG_H
#define PROTOTYPE_PERSISTED_LOG_H

#include <velocypack/Buffer.h>
#include "scheduler.h"

#include <velocypack/velocypack-aliases.h>
#include <mutex>
#include <utility>

#include "futures.h"
#include "rocksdb-handle.h"
#include "rocksdb-transaction.h"

namespace plog {

using log_index_type = uint64_t;
using log_id_type = uint64_t;



struct persisted_log {
  persisted_log(log_id_type logId, std::shared_ptr<RocksDBHandle> rocks,
                std::shared_ptr<sched::scheduler> scheduler);

  struct InsertResult {
    log_index_type index;
  };

  struct InsertIterator {
    virtual ~InsertIterator() = default;
    virtual auto next() -> std::optional<VPackBufferUInt8> = 0;
    virtual void reset() = 0;
  };

  auto append(log_index_type insert_after, std::unique_ptr<InsertIterator> iter)
      -> futures::future<InsertResult>;

  auto seek(log_index_type ) -> void;

 private:
  struct InsertRequest {
    log_index_type insert_after;
    std::unique_ptr<InsertIterator> iter;
    futures::promise<InsertResult> promise;

    InsertRequest(log_index_type insertAfter, std::unique_ptr<InsertIterator> iter,
                  futures::promise<InsertResult> promise);
  };

  auto handleInsertRequest(log_index_type insert_after,
                           std::unique_ptr<InsertIterator> iter) -> InsertResult;

  void runStoreOperation();
  void triggerStoreOperation();

  std::mutex _mutex;

  std::deque<InsertRequest> _requests;
  bool storeOperationRunning = false;

  futures::future<void> storeOperation = futures::make_fulfilled_promise();
  log_index_type _current_index = 0;
  log_id_type const _log_id;
  std::shared_ptr<RocksDBHandle> _rocks;
  std::shared_ptr<sched::scheduler> _scheduler;
};

}  // namespace plog

#endif  // PROTOTYPE_PERSISTED_LOG_H
