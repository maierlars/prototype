#include "persisted-log.h"

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

uint64_t read_big_endian_uint64(rocksdb::Slice slice) {
  uint64_t r = 0;
  for (auto i : {0, 1, 2, 3, 4, 5, 6, 7}) {
    r |= slice[i] << (8 * (7 - i));
  }
  return r;
}

}  // namespace

plog::persisted_log::persisted_log(plog::log_id_type logId,
                                   std::shared_ptr<RocksDBHandle> rocks,
                                   std::shared_ptr<sched::scheduler> scheduler)
    : _scheduler(std::move(scheduler)), _rocks(std::move(rocks)), _log_id(logId) {
  rocksdb::ReadOptions opts;
  auto iter = _rocks->db->NewIterator(opts, _rocks->logs.get());

  {
    std::string prefix;
    insert_big_endian_uint64(prefix, _log_id + 1);

    iter->SeekForPrev(rocksdb::Slice(prefix));
    auto status = iter->status();
    if (!status.ok()) {
      throw RocksDBException(status);
    }
  }

  {
    std::string prefix;
    insert_big_endian_uint64(prefix, _log_id);

    auto last_entry = iter->key();
    if (last_entry.starts_with(prefix)) {
      last_entry.remove_prefix(prefix.length());
      _current_index = read_big_endian_uint64(last_entry);
    } else {
      // nothing found, log is empty
      _current_index = 0;
    }
  }
}

auto plog::persisted_log::append(plog::log_index_type insert_after,
                                 std::unique_ptr<InsertIterator> iter)
    -> futures::future<InsertResult> {
  std::unique_lock guard(_mutex);
  auto&& [f, p] = futures::make_promise<InsertResult>();
  _requests.emplace_back(insert_after, std::move(iter), std::move(p));
  triggerStoreOperation();
  return std::move(f);
}

auto plog::persisted_log::handleInsertRequest(plog::log_index_type insert_after,
                                              std::unique_ptr<InsertIterator> iter)
    -> plog::persisted_log::InsertResult {
  rocksdb::WriteBatch batch;

  if (insert_after > _current_index) {
    throw std::logic_error("invalid index");
  }

  while (auto e = iter->next()) {
    insert_after += 1;

    std::string key;
    insert_big_endian_uint64(key, _log_id);
    insert_big_endian_uint64(key, insert_after);

    auto& entry = e.value();

    batch.Put(_rocks->logs.get(), rocksdb::Slice(key),
              rocksdb::Slice(reinterpret_cast<const char*>(entry.data()), entry.size()));
  }

  // delete the tail
  if (insert_after + 1 <= _current_index) {
    std::string from;
    insert_big_endian_uint64(from, _log_id);
    insert_big_endian_uint64(from, insert_after + 1);

    std::string to;
    insert_big_endian_uint64(from, _log_id + 1);
    batch.DeleteRange(_rocks->logs.get(), rocksdb::Slice(from), rocksdb::Slice(to));
  }

  rocksdb::WriteOptions opts;
  opts.sync = true;
  auto status = _rocks->db->Write(opts, batch.GetWriteBatch());
  if (!status.ok()) {
    abort();
  }

  _current_index = insert_after;
  return InsertResult{insert_after};
}

void plog::persisted_log::runStoreOperation() {
  std::unique_lock guard(_mutex);
  while (!_requests.empty()) {
    auto req = std::move(_requests.front());
    _requests.pop_front();

    guard.unlock();
    std::move(req.promise).capture([&] {
      return handleInsertRequest(req.insert_after, std::move(req.iter));
    });
    guard.lock();
  }

  assert(!_requests.empty());
}

void plog::persisted_log::triggerStoreOperation() {
  if (!storeOperationRunning) {
    storeOperationRunning = true;
    _scheduler->async([this] { return runStoreOperation(); }).finally([this](auto&& e) noexcept {
      std::unique_lock guard(_mutex);
      storeOperationRunning = false;
      if (e.has_error()) {
        abort();
      }
    });
  }
}

plog::persisted_log::InsertRequest::InsertRequest(plog::log_index_type insertAfter,
                                                  std::unique_ptr<InsertIterator> iter,
                                                  futures::promise<InsertResult> promise)
    : insert_after(insertAfter), iter(std::move(iter)), promise(std::move(promise)) {}
