//
// Created by lars on 02/12/2020.
//

#include "futures-2.h"
#include <chrono>
#include <thread>

using namespace futures::v2;


detail::invalid_pointer_type detail::invalid_pointer_promise_abandoned;
detail::invalid_pointer_type detail::invalid_pointer_promise_fulfilled;
detail::invalid_pointer_type detail::invalid_pointer_future_abandoned;


auto test() noexcept -> future<bool> {
  auto&& [f, p] = make_promise<bool>();

  std::thread t([p = std::move(p)]() mutable noexcept {
    using namespace std::chrono_literals;
    std::this_thread::sleep_for(10ms);

    std::move(p).fulfill(false);
  });
  t.detach();

  return std::move(f);
}

/*
auto bar(int x = 1000) -> future<int> {
  return join(test().and_then([x](bool ok) noexcept {
    if (ok) {
      return future<int>{std::in_place, 1};
    } else if (x == 0) {
      return future<int>{std::in_place, 12};
    }
    return bar(x - 1);
  }));
}*/

void baz_internal(promise<int>&& p, int x = 1000) {
  test().finally([p = std::move(p), x](bool ok) mutable noexcept {
    if (ok) {
      std::move(p).fulfill(1);
    } else if (x == 0) {
      std::move(p).fulfill(12);
    } else {
      baz_internal(std::move(p), x - 1);
    }
  });
}

auto baz() -> future<int> {
  auto&& [f, p] = make_promise<int>();
  baz_internal(std::move(p));
  return std::move(f);
}
/*
template <typename T>
struct retry_result : private detail::value_store<T> {
  template <typename... Args>
  static retry_result value(Args&&... args) {
    return retry_result(std::in_place, std::forward<Args>(args)...);
  }
  static retry_result retry() { return retry_result(); }

  using value_type = T;

 private:
  retry_result() : _retry(true) {}
  template <typename... Args>
  explicit retry_result(std::in_place_t, Args&&... args)
      : detail::value_store<T>(std::in_place, std::forward<Args>(args)...),
        _retry(false) {}

  template <typename F, typename S>
  friend void retry_internal(F&&, promise<S>&&);

  [[nodiscard]] bool is_retry() const { return _retry; }

  const bool _retry = false;
};

template <typename T>
struct is_retry_result : std::false_type {};
template <typename T>
struct is_retry_result<retry_result<T>> : std::true_type {};
template <typename T>
inline constexpr auto is_retry_result_v = is_retry_result<T>::value;

template <typename T>
struct is_future_retry : std::false_type {};
template <typename T>
struct is_future_retry<future<retry_result<T>>> : std::true_type {};
template <typename T>
inline constexpr auto is_future_retry_v = is_future_retry<T>::value;

template <typename F, typename T>
void retry_internal(F&& fn, promise<T>&& p) {
  std::invoke(std::forward<F>(fn))
      .finally([f = std::forward<F>(fn), p = std::move(p)](retry_result<T>&& res) mutable noexcept {
        if (res.is_retry()) {
          try {
            retry_internal(std::move(f), std::move(p));
          } catch (...) {
          }
        } else {
          if constexpr (std::is_void_v<T>) {
            std::move(p).fulfill();
          } else {
            std::move(p).fulfill(std::move(res._value));
          }
        }
      });
}

template <typename F, std::enable_if_t<std::is_nothrow_invocable_v<F>, int> = 0, typename R = std::invoke_result_t<F>,
          std::enable_if_t<is_future_retry_v<R>, int> = 0, typename B = typename R::result_type::value_type>
auto retry(F&& fn) -> future<B> {
  auto&& [f, p] = make_promise<B>();
  retry_internal(std::forward<F>(fn), std::move(p));
  return std::move(f);
}

auto foo(int x = 100) {
  return retry([x = x]() mutable noexcept -> future<retry_result<int>> {
    return test().and_then([x = x--](bool ok) mutable noexcept {
      if (ok) {
        return retry_result<int>::value(1);
      } else if (x == 0) {
        return retry_result<int>::value(12);
      } else {
        return retry_result<int>::retry();
      }
    });
  });
}*/

auto boo() -> future<void>;

int main(int argc, char* argv[]) {
  baz().finally([](int x) noexcept {
    std::cout << "end of futures " << x << std::endl;
    abort();
  });

  boo().and_then([]{
    std::cout << "hello world" << std::endl;
  });

  using namespace std::chrono_literals;
  std::this_thread::sleep_for(20s);
}
