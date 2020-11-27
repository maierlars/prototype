
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
#include "scheduler.h"

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
              if (auto value = v.unwrap(); value) {
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
  server.resource["^/replication/([^/]+)/append-entries$"]["POST"] =
      [api](std::shared_ptr<HttpServer::Response> response,
            std::shared_ptr<HttpServer::Request> request) {
        std::cout << "received append entries for log "
                  << request->path_match[1] << std::endl;
        futures::capture_into_future([&] {
          VPackBuilder builder;
          {
            VPackParser parser(builder);
            parser.parse(request->content.string());
          }

          return api->appendEntries(rlog::AppendEntriesRequest{builder.slice()});
        }).finally([response](auto&& e) noexcept {
          try {
            VPackBuilder builder;
            e.unwrap().intoBuilder(builder);
            response->write(SimpleWeb::StatusCode::success_ok, builder.toJson());
          } catch (std::exception const& ex) {
            response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                            ex.what());
          } catch (...) {
            response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
          }
        });
      };
}

void RegisterLeadershipHandler(HttpServer& server,
                               std::shared_ptr<rlog::ReplicatedLogLeadershipAPI> api) {
  server.resource["^/assume-leadership$"]["POST"] =
      [api](std::shared_ptr<HttpServer::Response> response,
            std::shared_ptr<HttpServer::Request> request) {
        futures::capture_into_future([api, request] {
          VPackBuilder builder;
          {
            VPackParser parser(builder);
            parser.parse(request->content.string());
          }

          api->assumeLeadership(builder.slice().get("term").getNumber<rlog::log_term_type>());
        }).finally([response](expect::expected<void>&& e) noexcept {
          try {
            e.rethrow_error();
            response->write(SimpleWeb::StatusCode::success_ok);
          } catch (std::exception const& ex) {
            response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                            ex.what());
          } catch (...) {
            response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
          }
        });
      };
}

void RegisterAppendHandler(HttpServer& server,
                           std::shared_ptr<rlog::ReplicatedLogTrxAPI> api) {
  server.resource["^/append"]["POST"] = [api](std::shared_ptr<HttpServer::Response> response,
                                              std::shared_ptr<HttpServer::Request> request) {
    futures::capture_into_future([api, request] {
      std::cout << "received append request from client" << std::endl;
      VPackBufferUInt8 payload;
      {
        VPackBuilder builder(payload);

        VPackParser parser(builder);
        parser.parse(request->content.string());
      }

      auto [index, term] = api->insert(std::move(payload));

      return api->waitFor(index, term);
    }).finally([response = std::move(response)](expected<rlog::ReplicatedLogTrxAPI::WaitResult>&& e) noexcept {
      if (e.has_error()) {
        try {
          std::rethrow_exception(e.error());
        } catch (std::exception const& ex) {
          response->write(SimpleWeb::StatusCode::server_error_internal_server_error,
                          ex.what());
        } catch (...) {
          response->write(SimpleWeb::StatusCode::server_error_internal_server_error);
        }
      } else {
        response->write(SimpleWeb::StatusCode::success_ok);
      }
    });
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

struct HTTPError : std::exception {
  explicit HTTPError(SimpleWeb::error_code error) : error(error) {}

  [[nodiscard]] const char* what() const noexcept override {
    return "http error";
  }

  SimpleWeb::error_code error;
};

struct RemoteLogParticipant : rlog::ParticipantAPI {
  explicit RemoteLogParticipant(std::string const& endpoint)
      : client(endpoint), endpoint(endpoint) {}

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

    std::cout << "Replicating to " << endpoint << std::endl;
    client.request("POST", "/replication/" + logIdentifier + "/append-entries", body,
                   [ctx](std::shared_ptr<HttpClient::Response> resp,
                         const SimpleWeb::error_code& err) mutable noexcept {
                     std::cout << "replication request returned " << err << std::endl;

                     if (err) {
                       std::move(ctx->p).throw_into<HTTPError>(err);
                     } else {
                       std::move(ctx->p).capture([&]() -> rlog::AppendEntriesResult {
                         VPackBufferUInt8 buffer;
                         {
                           auto str = resp->content.string();
                           VPackBuilder builder(buffer);
                           VPackParser parser(builder);
                           parser.parse(str);

                           std::cout << "response for replication is " << str << std::endl;
                         }

                         VPackSlice slice(buffer.data());
                         return rlog::AppendEntriesResult(slice);
                       });
                     }
                   });
    client.io_service->run();
    return std::move(f);
  }

  std::string logIdentifier = "abc";
  HttpClient client;
  std::string endpoint;
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

      std::cout << "Writing " << e->get().index << " " << e->get().term << std::endl;

      batch.Put(key.str(),
                rocksdb::Slice(reinterpret_cast<const char*>(e->get().payload.data()),
                               e->get().payload.size()));
    }
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;  // can we do this async?
    db->Write(write_opts, &batch);
    std::cout << "Sync done" << std::endl;
    return futures::make_fulfilled_promise<rlog::AppendEntriesResult>(std::in_place,
                                                                      req.term, true);
  }

 private:
  std::string key_prefix;
  std::shared_ptr<rocksdb::DB> db;
};

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "expected at least one parameter" << std::endl;
  }

  std::cout << "trollolol" << std::endl;
  auto ctx = std::make_shared<Context>();
  std::cout << "opening rocksdb at " << argv[1] << std::endl;
  ctx->db = OpenRocksDB(argv[1]);

  std::cout << "listening at port " << argv[2] << std::endl;

  HttpServer server;
  server.config.port = std::atoi(argv[2]);

  // auto rlog = std::make_shared<rlog::ReplicatedLog>();
  // RegisterReplicationHandler(server, rlog);

  auto scheduler = std::make_shared<sched::scheduler>();

  using namespace std::chrono_literals;

  scheduler
      ->delay(
          5s, [](int x) { return 2 * x; }, 12)
      .finally([](expected<int>&& e) noexcept {
        std::cout << e.unwrap() << std::endl;
      });

  std::vector<std::shared_ptr<rlog::ParticipantAPI>> parts;

  for (size_t i = 3; i < argc; i++) {
    std::cout << "adding " << argv[i] << " as participant" << std::endl;
    parts.push_back(std::make_shared<RemoteLogParticipant>(argv[i]));
  }

  parts.push_back(std::make_shared<LocalLogParticipant>("log/", ctx->db));
  auto log = std::make_shared<rlog::ReplicatedLog>(parts, scheduler);

  RegisterAppendHandler(server, log);
  RegisterReplicationHandler(server, log);
  RegisterLeadershipHandler(server, log);

  constexpr auto number_of_threads = 2;
  std::vector<std::thread> threads(number_of_threads);
  for (size_t i = 0; i < number_of_threads; i++) {
    threads.emplace_back([&] { scheduler->run(); });
  }

  auto doc_api = std::make_shared<SimpleDocumentAPI>();
  DocumentAPI::RegisterHandler(server, doc_api);

  server.start();
  return EXIT_SUCCESS;
}
