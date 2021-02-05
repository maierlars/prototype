
#include <client_http.hpp>
#include <iomanip>
#include <memory>
#include <mutex>
#include <server_http.hpp>

#include <velocypack/vpack.h>

#include <boost/lexical_cast.hpp>
#include <utility>

#include "rocksdb-handle.h"
#include "futures.h"
#include "replicated-log.h"
#include "scheduler.h"

#include "persisted-log.h"

using namespace futures;

using HttpServer = SimpleWeb::Server<SimpleWeb::HTTP>;
using HttpClient = SimpleWeb::Client<SimpleWeb::HTTP>;


struct Context : std::enable_shared_from_this<Context> {
  std::shared_ptr<RocksDBHandle> rocks;
};

using TransactionId = uint64_t;

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
  explicit LocalLogParticipant(std::string key_prefix, std::shared_ptr<RocksDBHandle> db)
      : key_prefix(std::move(key_prefix)), rocks(std::move(db)) {}

  futures::future<rlog::AppendEntriesResult> appendEntries(const rlog::AppendEntriesRequest& req) override {
    rocksdb::WriteBatch batch;
    while (auto e = req.entries->next()) {
      std::stringstream key;
      key << key_prefix;
      key << std::setw(16) << std::setfill('0') << std::hex << e->get().index;

      std::cout << "Writing " << e->get().index << " " << e->get().term << std::endl;

      batch.Put(rocks->logs.get(), key.str(),
                rocksdb::Slice(reinterpret_cast<const char*>(e->get().payload.data()),
                               e->get().payload.size()));
    }
    rocksdb::WriteOptions write_opts;
    write_opts.sync = true;  // can we do this async?
    rocks->db->Write(write_opts, &batch);
    std::cout << "Sync done" << std::endl;
    return futures::make_fulfilled_promise<rlog::AppendEntriesResult>(std::in_place,
                                                                      req.term, true);
  }

 private:
  std::string key_prefix;
  std::shared_ptr<RocksDBHandle> rocks;
};

int main(int argc, char* argv[]) {
  if (argc < 3) {
    std::cout << "expected at least one parameter" << std::endl;
  }

  std::cout << "trollolol" << std::endl;
  auto ctx = std::make_shared<Context>();
  std::cout << "opening rocksdb at " << argv[1] << std::endl;
  ctx->rocks = OpenRocksDB(argv[1]);

  std::cout << "listening at port " << argv[2] << std::endl;

  HttpServer server;
  server.config.port = std::atoi(argv[2]);

  // auto rlog = std::make_shared<rlog::ReplicatedLog>();
  // RegisterReplicationHandler(server, rlog);

  auto scheduler = std::make_shared<sched::scheduler>();

/*
  std::vector<std::shared_ptr<rlog::ParticipantAPI>> parts;

  for (size_t i = 3; i < argc; i++) {
    std::cout << "adding " << argv[i] << " as participant" << std::endl;
    parts.push_back(std::make_shared<RemoteLogParticipant>(argv[i]));
  }

  parts.push_back(std::make_shared<LocalLogParticipant>("log/", ctx->rocks));
  auto log = std::make_shared<rlog::ReplicatedLog>(parts, scheduler);

  RegisterAppendHandler(server, log);
  RegisterReplicationHandler(server, log);
  RegisterLeadershipHandler(server, log);

  constexpr auto number_of_threads = 2;
  std::vector<std::thread> threads(number_of_threads);
  for (size_t i = 0; i < number_of_threads; i++) {
    threads.emplace_back([&] { scheduler->run(); });
  }

  server.start();*/
  return EXIT_SUCCESS;
}
