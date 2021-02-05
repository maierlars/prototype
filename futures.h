#ifndef PROTOTYPE_FUTURES_H
#define PROTOTYPE_FUTURES_H
#include <atomic>
#include <function2/function2.hpp>
#include <tuple>

#include "expected.h"

namespace futures {

using namespace expect;

template <typename T>
struct future;
template <typename T>
struct promise;

template <typename T>
struct is_future : std::false_type {};
template <typename T>
struct is_future<future<T>> : std::true_type {};
template <typename T>
inline constexpr auto is_future_v = is_future<T>::value;

template <typename T>
auto make_promise() -> std::pair<future<T>, promise<T>>;
template <typename T>
future<T> make_fulfilled_promise(expected<T> e);
static future<void> make_fulfilled_promise();
template <typename T, typename... Args, std::enable_if_t<std::is_constructible_v<T, Args...>, int> = 0>
future<T> make_fulfilled_promise(std::in_place_t, Args&&...);
template <typename T, typename E, typename... Args>
future<T> make_failed_promise(Args&&... args);
template <typename T>
future<T> make_failed_promise(std::exception_ptr ptr);


template <typename T>
future<T> join(future<future<T>>&&);

namespace detail {

template <typename T>
struct future_base_type {
  using type = T;
};
template <typename T>
struct future_base_type<future<T>> {
  using type = T;
};
template <typename T>
using future_base_type_t = typename future_base_type<T>::type;

template <typename T>
struct shared_state {
  enum class state { EMPTY, VALUE, CALLBACK, DONE, ABANDONED };

  shared_state() : _state(state::EMPTY){};
  template <typename... Args>
  explicit shared_state(std::in_place_t, Args&&... args)
      : _state(state::VALUE) {
    new (&_store) expected<T>(std::in_place, std::forward<Args>(args)...);
  }
  explicit shared_state(expected<T> e)
      : _state(state::VALUE), _store(std::move(e)) {}
  explicit shared_state(const std::exception_ptr& ptr) : _state(state::VALUE) {
    new (&_store) expected<T>(ptr);
  }

  ~shared_state() {
    switch (_state) {
      case state::EMPTY:
      case state::CALLBACK:
      case state::ABANDONED:
        break;
      default:
        _store.~expected<T>();
    }
  }

  template <typename F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, F, expected<T>&&>, int> = 0>
  void set_callback(F&& f) {
    _callback = std::forward<F>(f);
    if (auto next = state::EMPTY; !_state.compare_exchange_strong(next, state::CALLBACK)) {
      if (next == state::VALUE) {
        std::invoke(_callback, std::move(_store));
      } else {
        throw std::runtime_error("invalid future/promise state");
      }
    }
  }

  template <typename... Args>
  void set_value(Args&&... args) {
    new (&_store) expected<T>(std::forward<Args>(args)...);
    if (auto next = state::EMPTY; !_state.compare_exchange_strong(next, state::VALUE)) {
      if (next == state::CALLBACK) {
        std::invoke(_callback, std::move(_store));
      } else {
        throw std::runtime_error("invalid future/promise state");
      }
    }
  }

 private:
  std::atomic<state> _state = state::EMPTY;
  fu2::unique_function<void(expected<T>&&) noexcept> _callback;
  union {
    expected<T> _store;
  };
};

template <typename T>
using shared_state_ptr = std::shared_ptr<shared_state<T>>;

template <typename T>
auto build_future(shared_state_ptr<T> ptr) -> future<T>;
template <typename T>
auto build_promise(shared_state_ptr<T> ptr) -> promise<T>;

template <typename T>
struct shared_base {
  shared_base() = delete;
  shared_base(shared_base const&) = delete;
  shared_base& operator=(shared_base const&) = delete;
  shared_base(shared_base&&) noexcept = default;
  shared_base& operator=(shared_base&&) noexcept = default;

  ~shared_base() = default;

  template <typename F, std::enable_if_t<std::is_invocable_r_v<void, F, shared_state<T>&>, int> = 0,
            typename R = std::invoke_result_t<F, shared_state<T>&>>
  void use(F&& f) && {
    if (_shared_state) {
      std::invoke(std::forward<F>(f), *_shared_state);
      _shared_state.reset();
    } else {
      throw std::runtime_error("future/promise already used");
    }
  }
  // make this protected
  shared_state_ptr<T> extract() && { return std::move(_shared_state); }

  [[nodiscard]] bool is_active() const { return _shared_state != nullptr; }

 protected:
 private:
  friend auto build_future<T>(shared_state_ptr<T> ptr) -> future<T>;
  friend auto build_promise<T>(shared_state_ptr<T> ptr) -> promise<T>;

  template <typename S>
  friend class future;
  template <typename S>
  friend class promise;

  explicit shared_base(shared_state_ptr<T> ptr)
      : _shared_state(std::move(ptr)) {}

  shared_state_ptr<T> _shared_state;
};

template <typename T>
auto build_future(shared_state_ptr<T> ptr) -> future<T> {
  return future<T>(std::move(ptr));
}
template <typename T>
auto build_promise(shared_state_ptr<T> ptr) -> promise<T> {
  return promise<T>(std::move(ptr));
}

template <typename T>
struct future_base : detail::shared_base<T> {
  using detail::shared_base<T>::shared_base;

  using base_type = T;

  expected<T> get() &&;

  template <typename F, std::enable_if_t<std::is_invocable_v<F, expected<T>&&>, int> = 0,
            typename S = std::invoke_result_t<F, expected<T>&&>, typename R = detail::future_base_type_t<S>>
  auto then(F&& f) && -> future<R> {
    auto [ff, p] = make_promise<R>();
    auto callback = [p = std::move(p), f = std::forward<F>(f)](expected<T>&& e) mutable noexcept {
      if constexpr (is_future_v<S>) {
        // this is expected<future<R>>
        auto r = captured_invoke(std::forward<F>(f), std::move(e));
        if (r.has_error()) {
          // fulfill with this error
          std::move(p).fulfill(r.error());
        } else {
          auto state = std::move(r).unwrap().extract();
          if (state == nullptr) {
            std::move(p).template throw_into<std::runtime_error>(
                "future in bad state");
          } else {
            static_assert(std::is_same_v<decltype(p), promise<R>>);
            std::move(*state).set_callback([p = std::move(p)](expected<R>&& e) mutable noexcept {
              std::move(p).fulfill(std::move(e));
            });
          }
        }
      } else {
        std::move(p).capture(std::forward<F>(f), std::move(e));
      }
    };

    std::move(*this).use([&](detail::shared_state<T>& state) {
      state.set_callback(std::move(callback));
    });
    return std::move(ff);
  }

  template <typename F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, F, expected<T>&&>, int> = 0>
  void finally(F&& f) && {
    std::move(*this).use([f = std::forward<F>(f)](detail::shared_state<T>& state) mutable {
      std::move(state).set_callback(std::move(f));
    });
  }

  template <typename E, typename... Args>
  auto throw_nested(Args&&... args) -> future<T> {
    return std::move(*this).then(
        [args = std::make_tuple(std::forward<Args>(args)...)](expected<T>&& e) mutable {
          try {
            return std::move(e).unwrap();
          } catch (...) {
            std::throw_with_nested(std::make_from_tuple<E>(std::move(args)));
          }
        });
  }

  template <typename E, typename F, typename... Args>
  auto throw_nested_if(Args&&... args) -> future<T> {
    return std::move(*this).then(
        [args = std::make_tuple(std::forward<Args>(args)...)](expected<T>&& e) mutable {
          try {
            return e.get();
          } catch (F const& e) {
            std::throw_with_nested(std::make_from_tuple<E>(std::move(args)));
          }
        });
  }
};

template <typename T>
struct promise_base : shared_base<T> {
  using detail::shared_base<T>::shared_base;

  promise_base(promise_base const&) = delete;
  promise_base& operator=(promise_base const&) = delete;

  promise_base(promise_base&&) noexcept = default;
  promise_base& operator=(promise_base&&) noexcept = default;
  ~promise_base() {
    if (this->is_active()) {
      std::terminate();
    }
  }

  void fulfill(expected<T> v) && noexcept(std::is_nothrow_move_constructible_v<expected<T>>) {
    std::move(*this).use(
        [&](detail::shared_state<T>& state) { state.set_value(std::move(v)); });
  }

  template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
            typename R = std::invoke_result_t<F, Args...>,
            std::enable_if_t<std::is_same_v<R, T> || std::is_same_v<R, expected<T>>, int> = 0>
  void capture(F&& f, Args&&... args) && noexcept {
    std::move(*this).fulfill(
        captured_invoke(std::forward<F>(f), std::forward<Args>(args)...));
  }

  template <typename F, typename... Args,
            std::enable_if_t<std::is_invocable_r_v<future<T>, F, Args...>, int> = 0>
  void capture(F&& f, Args&&... args) && noexcept {
    static_assert(std::is_void_v<T> || std::is_nothrow_move_constructible_v<T>);

    detail::shared_state_ptr<T> shared;
    try {
      shared = std::invoke(std::forward<F>(f), std::forward<Args>(args)...).extract();
    } catch (...) {
      std::move(*this).fulfill(std::current_exception());
    }

    if (shared == nullptr) {
      std::move(*this).template throw_into<std::logic_error>(
          "invalid future state");
    } else {
      shared->set_callback([p = std::move(*this)](expected<T>&& e) mutable noexcept {
        std::move(p).fulfill(std::move(e));
      });
    }
  }

  template <typename E, typename... Args, std::enable_if_t<std::is_constructible_v<E, Args...>, int> = 0>
  void throw_into(Args&&... args) && noexcept {
    try {
      throw E(std::forward<Args>(args)...);
    } catch (...) {
      std::move(*this).fulfill(std::current_exception());
    }
  }
};

}  // namespace detail

template <typename T>
struct future : detail::future_base<T> {
  using detail::future_base<T>::future_base;

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T&&>, int> = 0,
            typename S = std::invoke_result_t<F, T&&>, typename R = detail::future_base_type_t<S>>
  auto then_value(F&& f) && -> future<R> {
    auto [ff, p] = make_promise<R>();
    auto callback = [p = std::move(p), f = std::forward<F>(f)](expected<T>&& e) mutable noexcept {
      if (e.has_error()) {
        try {
          std::move(p).fulfill(e.error());
          return;
        } catch (...) {
          std::abort();
        }
      }
      if constexpr (is_future_v<S>) {
        // this is expected<future<R>>
        auto r = captured_invoke(std::forward<F>(f), std::move(e).unwrap());
        if (r.has_error()) {
          // fulfill with this error
          std::move(p).fulfill(r.error());
        } else {
          auto state = std::move(r).unwrap().extract();
          if (state == nullptr) {
            std::move(p).template throw_into<std::runtime_error>(
                "future in bad state");
          } else {
            static_assert(std::is_same_v<decltype(p), promise<R>>);
            std::move(*state).set_callback([p = std::move(p)](expected<R>&& e) mutable noexcept {
              std::move(p).fulfill(std::move(e));
            });
          }
        }
      } else {
        std::move(p).capture(std::forward<F>(f), std::move(e).unwrap());
      }
    };

    std::move(*this).use([&](detail::shared_state<T>& state) {
      state.set_callback(std::move(callback));
    });
    return std::move(ff);
  }
};

template <>
struct future<void> : detail::future_base<void> {
  using detail::future_base<void>::future_base;

  template <typename F, std::enable_if_t<std::is_invocable_v<F>, int> = 0,
            typename S = std::invoke_result_t<F>, typename R = detail::future_base_type_t<S>>
  auto and_then(F&& f) && -> future<R> {
    return std::move(*this).then([f = std::forward<F>(f)](expected<void>&& e) {
      e.rethrow_error();
      return std::invoke(f);
    });
  }
};

template <typename T>
struct promise : detail::promise_base<T> {
  using detail::promise_base<T>::promise_base;
  using detail::promise_base<T>::fulfill;

  template <typename... Args, std::enable_if_t<std::is_constructible_v<T, Args...>, int> = 0>
  void fulfill(std::in_place_t, Args&&... args) && {
    auto shared = std::move(*this).extract();
    shared->set_value(std::in_place, std::forward<Args>(args)...);
  }
};

template <>
struct promise<void> : detail::promise_base<void> {
  using detail::promise_base<void>::promise_base;
  using detail::promise_base<void>::fulfill;

  void fulfill() && { std::move(*this).fulfill(expected<void>{}); }
};

static_assert(std::is_move_constructible_v<future<int>>);
static_assert(std::is_move_constructible_v<promise<int>>);

template <typename T>
std::pair<future<T>, promise<T>> make_promise() {
  auto shared = std::make_shared<detail::shared_state<T>>();
  return std::make_pair(detail::build_future(shared), detail::build_promise(shared));
}

template <typename T>
future<T> make_fulfilled_promise(expected<T> e) {
  return detail::build_future(std::make_shared<detail::shared_state<T>>(std::move(e)));
}

static future<void> make_fulfilled_promise() {
  return detail::build_future(std::make_shared<detail::shared_state<void>>(std::in_place));
}

template <typename T, typename... Args, std::enable_if_t<std::is_constructible_v<T, Args...>, int>>
future<T> make_fulfilled_promise(std::in_place_t, Args&&... args) {
  return detail::build_future(
      std::make_shared<detail::shared_state<T>>(std::in_place, std::forward<Args>(args)...));
}

template <typename T, typename E, typename... Args>
auto make_failed_promise(Args&&... args) -> future<T> {
  try {
    throw E(std::forward<Args>(args)...);
  } catch (...) {
    return detail::build_future(
        std::make_shared<detail::shared_state<T>>(std::current_exception()));
  }
}

template <typename T>
future<T> make_failed_promise(std::exception_ptr ptr) {
  return detail::build_future(std::make_shared<detail::shared_state<T>>(std::move(ptr)));
}

template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
          typename S = std::invoke_result_t<F, Args...>, typename R = detail::future_base_type_t<S>>
auto capture_into_future(F&& f, Args&&... args) -> future<R> {
  try {
    if constexpr (is_future_v<S>) {
      return std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
    } else if constexpr (std::is_void_v<S>) {
      std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
      return make_fulfilled_promise<void>({});
    } else {
      return make_fulfilled_promise<R>(
          std::invoke(std::forward<F>(f), std::forward<Args>(args)...));
    }
  } catch (...) {
    return make_failed_promise<R>(std::current_exception());
  }
}

template <typename T>
future<T> join(future<future<T>>&& ff) {
  return std::move(ff).then(
      [](expected<future<T>>&& ef) noexcept { return std::move(ef).get(); });
}

template <typename Iter, std::enable_if_t<is_future_v<typename Iter::value_type>, int> = 0,
    typename R = typename Iter::value_type::base_type>
auto fan_in(Iter begin, Iter end) -> future<std::vector<expected<R>>> {
  using result_vector = std::vector<expected<R>>;

  struct Context {
    promise<result_vector> p;
    result_vector v;
    ~Context() { std::move(p).fulfill(std::in_place, std::move(v)); }
    Context(promise<result_vector> p, std::size_t size)
        : p(std::move(p)), v(size) {}
  };

  auto&& [f, p] = make_promise<result_vector>();
  auto ctx = std::make_shared<Context>(std::move(p), std::distance(begin, end));

  std::size_t index = 0;
  for (auto it = begin; it != end; it++) {
    std::move(*it).finally([ctx, index = index++](auto&& e) noexcept {
      ctx->v[index] = std::move(e);
    });
  }

  return std::move(f);
}

}  // namespace futures

#endif  // PROTOTYPE_FUTURES_H
