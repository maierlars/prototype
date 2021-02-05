#ifndef FUTURES_BASE_H
#define FUTURES_BASE_H
#include "continuation.h"

namespace futures {
inline namespace v2 {

namespace detail {

template <typename T, template<typename> typename super>
struct future_base : private small_box<T> {
  explicit future_base(detail::continuation_base<T>* ptr) noexcept
      : _base(ptr) {}
  future_base() noexcept = default;
  future_base(future_base const&) = delete;
  future_base& operator=(future_base const&) = delete;
  future_base(future_base&&) noexcept = default;
  future_base& operator=(future_base&&) noexcept = default;

  template <typename... Args, std::enable_if_t<std::is_constructible_v<T, Args...>, int> = 0>
  explicit future_base(std::in_place_t, Args&&... args) {
    if constexpr (has_inlined_value) {
      small_box<T>::emplace(std::forward<Args>(args)...);
      _base = detail::invalid_pointer_future_inline_value_v<T>;
    } else {
      _base = new detail::continuation_start<T>(std::in_place, std::forward<Args>(args)...);
    }
  }

  ~future_base() {
    if (_base) {
      _base->abandon_future();
    }
  }

  [[nodiscard]] bool empty() const noexcept { return _base == nullptr; }

  static constexpr auto has_inlined_value = small_box<T>::has_value;

 protected:
  template <typename R, typename F>
  auto base_and_then(F&& f) -> super<R> {
    if (auto local = this->template access_local_value<F, R>(std::forward<F>(f));
        !local.empty()) {
      return local;
    }

    auto step = new detail::continuation_step<void, F, R>(std::forward<F>(f));
    _base->continue_here(step);
    _base = nullptr;
    return step;
  }

  template <typename F>
  auto base_finally(F&& f) {
    if (auto local = this->template access_local_value<F>(std::forward<F>(f));
        !local.empty()) {
      return local;
    }

    auto step = new detail::continuation_final<T, F>(std::forward<F>(f));
    _base->continue_here(step);
    _base = nullptr;
    return step;
  }

 private:
  template <typename F, typename R>
  auto access_local_value(F&& f) -> super<R> {
    if constexpr (!has_inlined_value) {
      return super<R>{};
    }

    if (_base == detail::invalid_pointer_future_inline_value_v<T>) {
      return invoke_on_inline_value<F, R>(std::forward<F>(f));
    }

    return super<R>{};
  }

  moving_ptr<detail::continuation_base<T>> _base = nullptr;

  template <typename F, typename R>
  auto invoke_on_inline_value(F&& f) noexcept -> super<R> {
    try {
      return super<R>(std::in_place, small_box<T>::move_into(std::forward<F>(f)));
    } catch (...) {
      using handler = unhandled_exception_handler<R>;
      return super<R>(std::in_place,
                            std::invoke(handler{}, std::current_exception()));
    }
  }
};

template <typename T>
struct promise_base {
  explicit promise_base(detail::continuation_start<T>* ptr) noexcept
      : _base(ptr) {}
  promise_base(promise_base const&) = delete;
  promise_base& operator=(promise_base const&) = delete;
  promise_base(promise_base&&) noexcept = default;
  promise_base& operator=(promise_base&&) noexcept = default;

  ~promise_base() {
    if (_base) {
      _base->abandon_promise();
    }
  }

 protected:
  moving_ptr<detail::continuation_start<T>> _base = nullptr;
};
}  // namespace detail
}  // namespace v2
}  // namespace futures

#endif  // FUTURES_BASE_H
