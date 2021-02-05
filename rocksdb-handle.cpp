#include "rocksdb-handle.h"


std::shared_ptr<RocksDBHandle> OpenRocksDB(std::string const& dbname) {
  rocksdb::DB* ptr;
  rocksdb::DBOptions opts;
  opts.create_if_missing = true;
  opts.create_missing_column_families = true;

  rocksdb::ColumnFamilyOptions defaultFamily;
  rocksdb::ColumnFamilyOptions logFamily;

  std::vector<rocksdb::ColumnFamilyDescriptor> families;
  families.emplace_back(rocksdb::kDefaultColumnFamilyName, defaultFamily);
  families.emplace_back("logs", logFamily);

  std::vector<rocksdb::ColumnFamilyHandle*> handles;

  auto status = rocksdb::DB::Open(opts, dbname, families, &handles, &ptr);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }

  std::unique_ptr<rocksdb::DB> db_ptr{ptr};
  std::unique_ptr<rocksdb::ColumnFamilyHandle> defs_ptr{handles[0]};
  std::unique_ptr<rocksdb::ColumnFamilyHandle> logs_ptr{handles[1]};

  return std::make_shared<RocksDBHandle>(std::move(db_ptr), std::move(defs_ptr),
      std::move(logs_ptr));
}
