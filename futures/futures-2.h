#ifndef PROTOTYPE_FUTURES_2_H
#define PROTOTYPE_FUTURES_2_H
#include <atomic>
#include <cassert>
#include <functional>
#include <iostream>
#include <memory>
#include <utility>

#include "base.h"

namespace futures {
inline namespace v2 {

template <typename T>
struct future;
template <typename T>
struct promise;

template<typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
    typename R = std::invoke_result_t<F, Args...>>
auto capture_into_future(F&& f) -> future<R> {

}

template <typename T>
struct future : detail::future_base<T, future> {
  using detail::future_base<T, future>::future_base;

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T&&>, int> = 0,
            typename R = std::invoke_result_t<F, T&&>>
  auto and_then(F&& f) -> future<R> {
    return detail::future_base<T, future>::base_and_then(std::forward<F>(f));
  }

  template <typename F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, F, T&&>, int> = 0>
  void finally(F&& f) {
    auto step = new detail::continuation_final<T, F>(std::forward<F>(f));
    detail::future_base<T, future>::_base->continue_here(step);
    detail::future_base<T, future>::_base = nullptr;
  }
};

template <>
struct future<void> : detail::future_base<void, future> {
  using detail::future_base<void, future>::future_base;

  template <typename F, std::enable_if_t<std::is_invocable_v<F>, int> = 0, typename R = std::invoke_result_t<F>>
  auto and_then(F&& f) -> future<R> {
    return detail::future_base<void, future>::base_and_then<R>(std::forward<F>(f));
  }

  template <typename F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, F>, int> = 0>
  void finally(F&& f) {
    return detail::future_base<void, future>::base_finally(std::forward<F>(f));
  }
};

template <typename T>
struct promise : detail::promise_base<T> {
  using detail::promise_base<T>::promise_base;

  template <typename... Args, std::enable_if_t<std::is_constructible_v<T, Args...>, int> = 0>
  void fulfill(Args&&... args) && noexcept(std::is_nothrow_constructible_v<T, Args...>) {
    detail::promise_base<T>::_base->emplace_and_complete(std::forward<Args>(args)...);
    detail::promise_base<T>::_base = nullptr;
  }
};

template <>
struct promise<void> : detail::promise_base<void> {
  using detail::promise_base<void>::promise_base;

  void fulfill() && noexcept {
    detail::promise_base<void>::_base->emplace_and_complete();
    detail::promise_base<void>::_base = nullptr;
  }
};

template<typename T>
auto make_promise() -> std::pair<future<T>, promise<T>> {
  auto ptr = new detail::continuation_start<T>();
  return std::make_pair(future<T>(ptr), promise<T>(ptr));
}

}  // namespace v2
}  // namespace futures

#endif  // PROTOTYPE_FUTURES_2_H
