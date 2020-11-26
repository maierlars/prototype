#ifndef PROTOTYPE_ROCKSDB_TRANSACTION_H
#define PROTOTYPE_ROCKSDB_TRANSACTION_H

#include <rocksdb/db.h>
#include <rocksdb/utilities/write_batch_with_index.h>
#include <velocypack/vpack.h>

#include <velocypack/velocypack-aliases.h>

struct RocksDBException : std::exception {
  explicit RocksDBException(rocksdb::Status status)
      : status(std::move(status)) {}

  [[nodiscard]] const char* what() const noexcept override {
    return "rocksdb exception";
  }

  rocksdb::Status status;
};

struct RocksdbTransaction {
  explicit RocksdbTransaction(std::shared_ptr<rocksdb::DB> db)
      : db(std::move(db)), snapshot(this->db.get()) {}
  virtual ~RocksdbTransaction() = default;

  std::optional<VPackBufferUInt8> Get(std::string_view key) {
    std::string value;
    rocksdb::ReadOptions opts;
    opts.snapshot = snapshot.snapshot();
    auto status = batch.GetFromBatchAndDB(db.get(), opts, key, &value);
    if (status.ok()) {
      auto result = std::optional<VPackBufferUInt8>{std::in_place};
      result->append(value);
      return result;
    } else if (status.IsNotFound()) {
      return std::nullopt;
    } else {
      throw RocksDBException(std::move(status));
    }
  }

  void Put(std::string_view key, VPackBufferUInt8 value) {
    auto status =
        batch.Put(key, rocksdb::Slice(reinterpret_cast<char const*>(value.data()),
                                      value.size()));
    if (!status.ok()) {
      throw RocksDBException(std::move(status));
    }
  }

  void Delete(std::string_view key) {
    auto status = batch.Delete(key);
    if (!status.ok()) {
      throw RocksDBException(std::move(status));
    }
  }

  void Write(rocksdb::WriteOptions opts = {}) {
    auto status = db->Write(opts, batch.GetWriteBatch());
    if (!status.ok()) {
      throw RocksDBException(std::move(status));
    }
  }

 protected:
  std::shared_ptr<rocksdb::DB> db;
  rocksdb::WriteBatchWithIndex batch;
  rocksdb::ManagedSnapshot snapshot;
};

#endif  // PROTOTYPE_ROCKSDB_TRANSACTION_H
