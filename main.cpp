
#include <client_http.hpp>
#include <iomanip>
#include <memory>
#include <mutex>
#include <server_http.hpp>

#include <rocksdb/db.h>
#include <rocksdb/write_batch.h>

#include <velocypack/vpack.h>

#include <boost/lexical_cast.hpp>
#include <utility>

#include "futures.h"
#include "replicated-log.h"

using namespace futures;

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;

std::shared_ptr<rocksdb::DB> OpenRocksDB(std::string const& dbname) {
  rocksdb::DB* ptr;
  rocksdb::Options opts;
  opts.create_if_missing = true;
  auto status = rocksdb::DB::Open(opts, dbname, &ptr);
  if (!status.ok()) {
    throw std::runtime_error(status.ToString());
  }

  return std::shared_ptr<rocksdb::DB>{ptr};
}

struct Context : std::enable_shared_from_this<Context> {
  std::shared_ptr<rocksdb::DB> db;
};

using TransactionId = uint64_t;

struct DocumentAPI {
  virtual ~DocumentAPI() = default;

  struct DocumentRequest {
    std::optional<TransactionId> transactionId;
    std::string collectionName;
    std::string key;

    DocumentRequest(std::shared_ptr<HttpServer::Request> const& req) {
      collectionName = req->path_match.operator[](0).str();
      key = req->path_match.operator[](1).str();

      if (auto it = req->header.find("Transaction-Id"); it != std::end(req->header)) {
        transactionId = boost::lexical_cast<TransactionId>(it->second);
      }
    }
  };

  struct DocumentPutRequest : DocumentRequest {
    std::string value;

    DocumentPutRequest(std::shared_ptr<HttpServer::Request> const& req)
        : DocumentRequest(req) {
      value = req->content.string();
    }
  };

  virtual auto Get(DocumentRequest request)
      -> futures::future<std::optional<std::string>> = 0;
  virtual auto Put(DocumentPutRequest request)
      -> futures::future<std::optional<std::string>> = 0;
  virtual auto Delete(DocumentRequest request)
      -> futures::future<std::optional<std::string>> = 0;

  static void RegisterHandler(HttpServer& server, std::shared_ptr<DocumentAPI> api) {
    server.resource["^/document/([^/]+)$"]["GET"] =
        [api](std::shared_ptr<HttpServer::Response> response,
              std::shared_ptr<HttpServer::Request> request) {
          futures::capture_into_future([api, request] {
            return api->Get(request);
          }).finally([response = std::move(response)](expect::expected<std::optional<std::string>> v) noexcept {
            try {
              if (auto value = v.get(); value) {
                response->write(SimpleWeb::StatusCode::success_ok, value.value());
              } else {
                response->write(SimpleWeb::StatusCode::client_error_not_found);
              }
            } catch (std::exception const& e) {
              response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                              e.what());
            } catch (...) {
              response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
            }
          });
        };

    server.resource["^/document/([^/]+)$"]["POST"] =
        [api](std::shared_ptr<HttpServer::Response> response,
              std::shared_ptr<HttpServer::Request> request) {
          response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                          "not implemented");
        };

    server.resource["^/document/([^/]+)$"]["DELETE"] =
        [api](std::shared_ptr<HttpServer::Response> response,
              std::shared_ptr<HttpServer::Request> request) {
          response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                          "not implemented");
        };
  }
};

void RegisterReplicationHandler(HttpServer& server,
                                std::shared_ptr<rlog::ReplicatedLogReplicationAPI> api) {

  if (!api) { std::abort(); }

  server.resource["^/replication/([^/]+)/append-entries$"]["POST"] =
      [api](std::shared_ptr<HttpServer::Response> response,
            std::shared_ptr<HttpServer::Request> request) {

        VPackBuilder builder;
        {
          VPackParser parser(builder);
          parser.parse(request->content.string());
        }

        auto result = api->appendEntries(rlog::AppendEntriesRequest{builder.slice()});
        builder.clear();
        result.intoBuilder(builder);
        response->write(SimpleWeb::StatusCode::success_ok, builder.toJson());
      };
}

struct SimpleDocumentAPI : DocumentAPI {
  futures::future<std::optional<std::string>> Get(DocumentRequest request) override {
    return futures::make_fulfilled_promise<std::optional<std::string>>(std::in_place,
                                                                       std::nullopt);
  }

  futures::future<std::optional<std::string>> Put(DocumentPutRequest request) override {}
  futures::future<std::optional<std::string>> Delete(DocumentRequest request) override {}
};

void RegisterReplicationRestHandler(HttpServer& server,
                                    std::shared_ptr<Context> const& ctx) {}

struct HTTPError : std::exception {
  explicit HTTPError(SimpleWeb::error_code error) : error(error) {}

  [[nodiscard]] const char* what() const noexcept override {
    return "http error";
  }

  SimpleWeb::error_code error;
};

struct RemoteLogParticipant : rlog::ParticipantAPI {
  explicit RemoteLogParticipant(std::string const& endpoint)
      : client(endpoint) {}

  futures::future<rlog::AppendEntriesResult> appendEntries(const rlog::AppendEntriesRequest& req) override {
    std::string body;
    {
      VPackBuilder builder;
      req.intoBuilder(builder);
      body = builder.toJson();
    }

    struct Context {
      using promise = futures::promise<rlog::AppendEntriesResult>;
      explicit Context(promise p) : p(std::move(p)) {}
      promise p;
    };

    auto [f, p] = futures::make_promise<rlog::AppendEntriesResult>();
    auto ctx = std::make_shared<Context>(std::move(p));

    client.request("POST", "/replication/" + logIdentifier + "/append-entries", body,
                   [ctx](std::shared_ptr<HttpClient::Response> resp,
                         const SimpleWeb::error_code& err) mutable noexcept {
                     if (err) {
                       std::move(ctx->p).except<HTTPError>(err);
                     } else {
                       std::move(ctx->p).capture([&]() -> rlog::AppendEntriesResult {
                         if (resp->status_code == "200" ||
                             resp->status_code == "403") {
                           VPackBufferUInt8 buffer;
                           {
                             VPackBuilder builder(buffer);
                             VPackParser parser(builder);
                             parser.parse(resp->content.string());
                           }

                           VPackSlice slice(buffer.data());
                           return rlog::AppendEntriesResult(slice);
                         } else {
                           throw std::logic_error(
                               "unexpected http status code");
                         }
                       });
                     }
                   });

    return std::move(f);
  }

  std::string logIdentifier;
  HttpClient client;
};

struct LocalLogParticipant : rlog::ParticipantAPI {
  explicit LocalLogParticipant(std::string key_prefix, std::shared_ptr<rocksdb::DB> db)
      : key_prefix(std::move(key_prefix)), db(std::move(db)) {}

  futures::future<rlog::AppendEntriesResult> appendEntries(const rlog::AppendEntriesRequest& req) override {
    rocksdb::WriteBatch batch;
    while (auto e = req.entries->next()) {
      std::stringstream key;
      key << key_prefix;
      key << std::setw(16) << std::setfill('0') << std::hex << e->get().index;

      batch.Put(key.str(),
                rocksdb::Slice(reinterpret_cast<const char*>(e->get().payload.data()),
                               e->get().payload.size()));
    }
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;  // can we do this async?
    db->Write(write_opts, &batch);

    return futures::make_fulfilled_promise<rlog::AppendEntriesResult>(std::in_place,
                                                                      req.term, true);
  }

 private:
  std::string key_prefix;
  std::shared_ptr<rocksdb::DB> db;
};

int main() {
  std::cout << "trollolol" << std::endl;
  auto ctx = std::make_shared<Context>();
  ctx->db = OpenRocksDB("db");

  HttpServer server;
  server.config.port = 8080;

  auto rlog = std::make_shared<rlog::ReplicatedLog>();
  RegisterReplicationHandler(server, rlog);

  (void) rlog;

  auto doc_api = std::make_shared<SimpleDocumentAPI>();
  DocumentAPI::RegisterHandler(server, doc_api);

  server.start();
  return EXIT_SUCCESS;
}
