#ifndef FUTURE_CONTINUATION_H
#define FUTURE_CONTINUATION_H
#include "box.h"
#include "handler.h"
#include "helper.h"

namespace futures {
inline namespace v2 {
namespace detail {

template <typename T>
struct continuation {
  virtual ~continuation() = default;
  virtual void operator()(devoidify_t<T>&&) noexcept = 0;
};
template <typename T>
struct continuation_base;

struct invalid_pointer_type {};

extern invalid_pointer_type invalid_pointer_promise_abandoned;
extern invalid_pointer_type invalid_pointer_promise_fulfilled;
extern invalid_pointer_type invalid_pointer_future_abandoned;
extern invalid_pointer_type invalid_pointer_future_inline_value;

template <typename T>
const auto invalid_pointer_promise_abandoned_v =
    reinterpret_cast<pointer_to_t<detail::continuation<T>>>(std::addressof(invalid_pointer_promise_abandoned));

template <typename T>
const auto invalid_pointer_promise_fulfilled_v =
    reinterpret_cast<pointer_to_t<detail::continuation<T>>>(std::addressof(invalid_pointer_promise_fulfilled));

template <typename T>
const auto invalid_pointer_future_abandoned_v =
    reinterpret_cast<pointer_to_t<detail::continuation<T>>>(std::addressof(invalid_pointer_future_abandoned));

template <typename T>
const auto invalid_pointer_future_inline_value_v =
    reinterpret_cast<pointer_to_t<detail::continuation_base<T>>>(std::addressof(invalid_pointer_future_inline_value));

template <typename T>
struct continuation_ref {
  using pointer_type = pointer_to_t<detail::continuation<T>>;

 protected:
  std::atomic<pointer_type> _next = nullptr;
};

template <typename T>
struct continuation_base : private continuation_ref<T>, detail::box<T> {
  using pointer_type = typename continuation_ref<T>::pointer_type;

  continuation_base() = default;

  template <typename... Args>
  explicit continuation_base(std::in_place_t, Args&&... args) {
    this->_next = invalid_pointer_promise_fulfilled_v<T>;
    detail::box<T>::emplace(std::forward<Args>(args)...);
  }

  template <typename... Args>
  void emplace_and_complete(Args&&... args) noexcept(
      noexcept(detail::box<T>::emplace(std::forward<Args>(args)...))) {
    detail::box<T>::emplace(std::forward<Args>(args)...);
    trigger_value_received();
  }

  template <typename F, typename... Args>
  void emplace_result_and_complete(F&& f, Args&&... args) noexcept(noexcept(
      detail::box<T>::emplace_result(std::forward<F>(f), std::forward<Args>(args)...))) {
    detail::box<T>::emplace_result(std::forward<F>(f), std::forward<Args>(args)...);
    trigger_value_received();
  }

  void continue_here(pointer_type next) noexcept {
    pointer_type expected = nullptr;
    bool success = continuation_ref<T>::_next.compare_exchange_strong(expected, next);
    if (!success) {
      if (expected == detail::invalid_pointer_promise_fulfilled_v<T>) {
        invoke_ptr(this->_next, *this);
        detail::box<T>::destroy();
      } else {
        invoke_abandoned_promise_handler(next);
      }
      delete next;
      delete this;
    }
  }

  template <typename R>
  bool continue_here(pointer_type next, box<R>& result_store) noexcept {
    pointer_type expected = nullptr;
    bool success = continuation_ref<T>::_next.compare_exchange_strong(expected, next);
    if (!success) {
      if (expected == detail::invalid_pointer_promise_fulfilled_v<T>) {
        result_store.emplace_result(
            [&](box<T>& t) {
              if constexpr (std::is_void_v<T>) {
                std::invoke(*next, unit_type{});
              } else {
                std::invoke(*next, std::move(t).ref());
              }
            },
            *this);
        detail::box<T>::destroy();
      } else {
        invoke_abandoned_promise_handler(next);
      }
      delete next;
      delete this;
      return true;
    }

    return false;
  }

  void abandon_promise() noexcept {
    pointer_type expected = nullptr;
    bool success = continuation_ref<T>::_next.compare_exchange_strong(
        expected, detail::invalid_pointer_promise_abandoned_v<T>);
    if (!success) {
      if (expected != detail::invalid_pointer_future_abandoned_v<T>) {
        invoke_abandoned_promise_handler(this->_next);
        delete this->_next;
      }
      delete this;
    }
  }

  void abandon_future() noexcept {
    pointer_type expected = nullptr;
    bool success = continuation_ref<T>::_next.compare_exchange_strong(
        expected, detail::invalid_pointer_future_abandoned_v<T>);
    if (!success) {
      if (expected != detail::invalid_pointer_promise_abandoned_v<T>) {
        invoke_abandoned_value_handler(*this);
        detail::box<T>::destroy();
      }
      delete this;
    }
  }

 private:
  static void invoke_abandoned_promise_handler(pointer_type ptr) noexcept {
    using handler = abandoned_promise_handler<T>;
    if constexpr (std::is_nothrow_invocable_r_v<void, handler> || std::is_void_v<T>) {
      std::invoke(handler{});
    } else {
      static_assert(std::is_nothrow_invocable_r_v<T, handler>);
      std::invoke(*ptr, std::invoke(handler{}));
    }
  }

  static void invoke_abandoned_value_handler(detail::box<T>& v) noexcept {
    using handler = abandoned_value_handler<T>;
    if constexpr (std::is_nothrow_invocable_r_v<void, handler, devoidify_t<T>&&>) {
      std::invoke(handler{}, std::move(v).ref());
    } else {
      static_assert(std::is_nothrow_invocable_r_v<void, handler>);
      std::invoke(handler{});
    }
  }

  void trigger_value_received() noexcept {
    pointer_type expected = nullptr;
    bool success = continuation_ref<T>::_next.compare_exchange_strong(
        expected, detail::invalid_pointer_promise_fulfilled_v<T>);
    if (!success) {
      if (expected != detail::invalid_pointer_future_abandoned_v<T>) {
        invoke_ptr(this->_next, *this);
      } else {
        invoke_abandoned_value_handler(*this);
      }
      detail::box<T>::destroy();
      delete this;
    }
  }

  static void invoke_ptr(pointer_type ptr, detail::box<T>& t) noexcept {
    if constexpr (std::is_void_v<T>) {
      std::invoke(*ptr, unit_type{});
    } else {
      std::invoke(*ptr, std::move(t).ref());
    }
  }
};

template <typename T, typename F, typename R>
struct continuation_step_base : continuation<T>, continuation_base<R>, F {
  explicit continuation_step_base(F f) : F(std::move(f)) {}
};

template <typename T, typename F, typename R>
struct continuation_step final : continuation_step_base<T, F, R> {
  explicit continuation_step(F f)
      : continuation_step_base<T, F, R>(std::move(f)) {}

  void operator()(devoidify_t<T>&& t) noexcept final {
    try {
      if constexpr (std::is_void_v<T>) {
        detail::box<T>::emplace_result(function_self());
      } else {
        detail::box<T>::emplace_result(function_self(), std::move(t));
      }
    } catch (...) {
      using handler = unhandled_exception_handler<T>;
      detail::box<T>::emplace(std::invoke(handler{}, std::current_exception()));
    }
    continuation_step_base<T, F, R>::trigger_value_received();
  }

 private:
  F& function_self() { return *this; }
};

template <typename T, typename F>
struct continuation_final final : continuation<T>, F {
  explicit continuation_final(F f) : F(std::move(f)) {}
  void operator()(devoidify_t<T>&& t) noexcept final {
    if constexpr (std::is_void_v<T>) {
      static_assert(std::is_nothrow_invocable_r_v<void, F>);
      std::invoke(function_self());
    } else {
      static_assert(std::is_nothrow_invocable_r_v<void, F, T&&>);
      std::invoke(function_self(), std::move(t));
    }
  }

 private:
  F& function_self() { return *this; }
};

template <typename T>
struct continuation_start final : continuation_base<T> {};

}  // namespace detail
}  // namespace v2
}  // namespace futures

#endif  // FUTURE_CONTINUATION_H
