#ifndef PROTOTYPE_ROCKSDB_HANDLE_H
#define PROTOTYPE_ROCKSDB_HANDLE_H
#include <memory>
#include <rocksdb/db.h>

struct RocksDBHandle {
  RocksDBHandle(std::unique_ptr<rocksdb::DB> db,
      std::unique_ptr<rocksdb::ColumnFamilyHandle> def,
      std::unique_ptr<rocksdb::ColumnFamilyHandle> logs)
      : db(std::move(db)), default_(std::move(def)), logs(std::move(logs)) {}

  std::unique_ptr<rocksdb::DB> db;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> default_;
  std::unique_ptr<rocksdb::ColumnFamilyHandle> logs;
};

std::shared_ptr<RocksDBHandle> OpenRocksDB(std::string const& dbname);

#endif  // PROTOTYPE_ROCKSDB_HANDLE_H
