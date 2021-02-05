#include <iostream>
#include <memory>

#include <boost/asio.hpp>
#include <boost/fiber/all.hpp>

namespace asio = boost::asio;
namespace fibers = boost::fibers;

const char request[] = "GET / HTTP/1.1\r\n\r\n";

struct my_interface {

  template<typename R>
  struct CompletionCallback {
    template<typename... Args, std::enable_if_t<std::is_constructible_v<R, Args...>, int> = 0>
    void operator()(Args&& ... args) const noexcept(std::is_nothrow_constructible_v<R, Args...>) {}
  };

  virtual void foo(int, CompletionCallback<int> callback) = 0;
};

struct my_interface_service : my_interface {

  void foo(int, CompletionCallback<int> callback) override {}
};



struct client_base : std::enable_shared_from_this<client_base> {
  virtual ~client_base() = default;
};

template <typename socket_type>
struct client : client_base {
  explicit client(asio::ip::tcp::socket sock) : _sock(std::move(sock)) {
    async_read_next_header();
  }

  struct request {
    std::size_t request_id = 0;
    std::size_t service_length = 0;
    std::size_t method_length = 0;
    std::size_t body_length = 0;

    std::string service_name;
    std::string method_name;

    std::unique_ptr<std::byte[]> body;
  };

  void async_read_next_header() {
    auto req = std::make_shared<request>();

    std::array<asio::mutable_buffer, 4> buffers = {
        asio::buffer(&req->request_id, sizeof(std::size_t)),
        asio::buffer(&req->service_length, sizeof(std::size_t)),
        asio::buffer(&req->method_length, sizeof(std::size_t)),
        asio::buffer(&req->body_length, sizeof(std::size_t)),
    };

    asio::async_read(_sock, buffers, asio::transfer_at_least(4 * sizeof(std::size_t)),
                     [this, self = shared_from_this(),
                      req = std::move(req)](boost::system::error_code ec,
                                            std::size_t bytesRead) mutable {
                       if (ec.failed()) {
                         std::cerr << "async read failed: " << ec << std::endl;
                       } else {
                         async_read_next_request(std::move(req));
                       }
                     });
  }

  void async_read_next_request(std::shared_ptr<request> req) {
    req->service_name.reserve(req->service_length);
    req->method_name.reserve(req->method_length);
    req->body = std::make_unique<std::byte[]>(req->body_length);

    std::array<asio::mutable_buffer, 3> buffers = {
        asio::buffer(req->service_name.data(), req->service_length),
        asio::buffer(req->method_name.data(), req->method_length),
        asio::buffer(req->body.get(), req->body_length),
    };

    asio::async_read(_sock, buffers,
                     asio::transfer_exactly(req->service_length +
                                            req->method_length + req->body_length),
                     [this, self = shared_from_this(),
                      req = std::move(req)](boost::system::error_code ec,
                                            std::size_t bytesRead) mutable {

                       std::cout << "received request " << req->request_id << std::endl;
                       std::cout << "call to " << req->service_name << "::" << req->method_name << std::endl;

                     });
  }

  asio::ip::tcp::socket _sock;
};

int main(int argc, char* argv[]) {
  auto io = std::make_shared<asio::io_context>();

  asio::ip::tcp::acceptor acceptor(*io);

  acceptor.async_accept([&](boost::system::error_code ec, asio::ip::tcp::socket sock) {
    if (ec.failed()) {
      std::cerr << "accept failed: " << ec << std::endl;
    } else {
      auto c = std::make_shared<client<decltype(sock)>>(std::move(sock));
    }
  });

  /*asio::ip::tcp::socket s(*io);
  asio::ip::tcp::resolver r(*io);


  char buffer[1024];

  auto const async_read_completion = [&](boost::system::error_code ec, std::size_t bytesRead){
    if (ec.failed()) {
      std::cerr << "read failed: " << ec << std::endl;
    } else {
      std::cout << "read completed " << bytesRead << std::endl;
      std::cout << buffer << std::endl;
    }
  };

  auto const async_write_completio

  auto const async_read_completion = [&](boost::system::error_code ec, std::size_t bytesRead){
    if (ec.failed()) {n = [&](boost::system::error_code ec,
      std::size_t bytesWritten) {
    if (ec.failed()) {
      std::cerr << "write failed: " << ec << std::endl;
    } else {
      std::cout << "write completed " << bytesWritten << std::endl;
      asio::async_read(s, asio::buffer(buffer), async_read_completion);
    }
  };

  auto const async_connect_handler = [&](boost::system::error_code const& ec,
                                         asio::ip::tcp::endpoint const& endpoint) {
    if (ec.failed()) {
      std::cerr << "connect failed: " << ec << std::endl;
    } else {
      std::cout << "connect completed " << endpoint << std::endl;
      asio::async_write(s, asio::buffer(request), async_write_completion);
    }
  };

  auto const async_resolve_handler = [&](boost::system::error_code ec,
                                         asio::ip::tcp::resolver::results_type const& result) {
    if (ec.failed()) {
      std::cerr << "resolution failed: " << ec << std::endl;
    } else {
      asio::async_connect(s, result, async_connect_handler);
    }
  };

  r.async_resolve("google.de", "http", async_resolve_handler);

  asio::deadline_timer timer(*io);
  timer.expires_from_now(boost::posix_time::milliseconds {1});
  timer.async_wait([&](boost::system::error_code ec) {
    std::cout << "cancel async socket" << std::endl;
    r.cancel();
    if (s.is_open()) {
      std::cout << "calling cancel on socket" << std::endl;
      s.cancel();
    }
  });*/

  io->run();
  std::cout << "io.run() returned" << std::endl;
  return EXIT_SUCCESS;
}
