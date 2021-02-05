#ifndef PROTOTYPE_DEVOIDIFY_H
#define PROTOTYPE_DEVOIDIFY_H
#include <type_traits>
#include <tuple>

namespace devoidify {

struct void_replacement_t {};

template<typename T>
inline constexpr auto is_void_replacement_v = std::is_same_v<std::decay_t<T>, void_replacement_t>;

template<typename T>
using revert = std::conditional_t<is_void_replacement_v<T>, void, T>;

template<typename T>
using type = std::conditional_t<std::is_void_v<T>, void_replacement_t, T>;

template<typename F, typename... Args,
    std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
    typename R = std::invoke_result_t<F, Args...>>
auto call_result(F&& f, Args&&... args) noexcept(std::is_nothrow_invocable_v<F, Args...>) -> type<R> {
  if constexpr (std::is_void_v<R>) {
    std::invoke(std::forward<F>(f), std::forward<Args...>);
    return void_replacement_t{};
  } else {
    return std::invoke(std::forward<F>(f), std::forward<Args...>);
  }
}

namespace detail {

template<typename F, typename... Args1>
auto call_fixed_filter(F&& f, Args1&&... args1) {
  return std::apply(std::forward<F>(f), std::forward<Args1>(args1)...);
}

template<typename F, typename Args1, typename Arg, typename... Args2>
auto call_fixed_filter(F&& f, Args1&& args1, Arg&& arg, Args2&&... args2) {
  if constexpr (is_void_replacement_v<Arg>) {
    return call_fixed_filter(std::forward<F>(f), std::forward<Args1>(args1), std::forward<Args2>(args2)...);
  } else {
    return call_fixed_filter(std::forward<F>(f), std::tuple_cat(std::forward<Args1>(args1), std::forward_as_tuple(std::forward<Arg>(arg))), std::forward<Args2>(args2)...);
  }
}
}


template<typename F, typename... Args>
auto call(F&& f, Args&&... args){
  using R = decltype(detail::call_fixed_filter(std::forward<F>(f), std::forward_as_tuple(), std::forward<Args>(args)...));
  if constexpr (std::is_void_v<R>) {
    detail::call_fixed_filter(std::forward<F>(f), std::forward_as_tuple(), std::forward<Args>(args)...);
    return void_replacement_t{};
  } else {
    return detail::call_fixed_filter(std::forward<F>(f), std::forward_as_tuple(), std::forward<Args>(args)...);
  }
}


}

#endif  // PROTOTYPE_DEVOIDIFY_H
