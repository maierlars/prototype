#include <rocksdb/db.h>
#include <iostream>
#include <memory>
#include <mutex>
#include <server_http.hpp>

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;

std::unique_ptr<rocksdb::DB> OpenRocksDB(std::string const& dbname) {
  rocksdb::DB* ptr;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  auto status = rocksdb::DB::Open(opts, dbname, &ptr);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }
  return std::unique_ptr<rocksdb::DB>{ptr};
}

void RegisterRestHandler(HttpServer& server, std::unique_ptr<rocksdb::DB> const& db) {
  server.resource["^/document/([^/]+)"]["GET"] =
      [&](std::shared_ptr<HttpServer::Response> response,
          std::shared_ptr<HttpServer::Request> request) {
        auto key = request->path_match[1].str();

        std::string value;
        rocksdb::ReadOptions opts;
        auto status = db->Get(opts, rocksdb::Slice(key), &value);
        if (status.IsNotFound()) {
          response->write(SimpleWeb::StatusCode::client_error_not_found);
        } else if(status.ok()) {
          response->write(SimpleWeb::StatusCode::success_ok, value);
        } else {
          response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
        }
      };
}

int main() {
  auto db = OpenRocksDB("db");

  HttpServer server;
  server.config.port = 8080;
  RegisterRestHandler(server, db);

  std::thread server_thread([&] { server.start(); });

  server_thread.join();
  server.stop();
  return EXIT_SUCCESS;
}
