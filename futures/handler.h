#ifndef FUTURES_HANDLER_H
#define FUTURES_HANDLER_H

namespace futures {
inline namespace v2 {
template <typename T>
struct abandoned_value_handler {
  void operator()() noexcept {}
};

template <typename T>
struct unhandled_exception_handler {
  T operator()(std::exception_ptr) noexcept { std::terminate(); }
};

template <typename T>
struct abandoned_promise_handler {
  void operator()() noexcept { std::terminate(); }
};

}  // namespace v2
}  // namespace futures

#endif  // FUTURES_HANDLER_H
