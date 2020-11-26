#ifndef PROTOTYPE_EXPECTED_H
#define PROTOTYPE_EXPECTED_H

#include <functional>
#include <stdexcept>
#include <utility>

namespace expect {

template <typename T>
struct expected;

template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0>
auto captured_invoke(F&& f, Args&&... args) noexcept
    -> expected<std::invoke_result_t<F, Args...>>;

namespace detail {

template <typename T>
struct expected_base {
  template <typename F, std::enable_if_t<std::is_invocable_v<F, expected<T>&>, int> = 0>
  auto map(F&& f) & noexcept -> expected<std::invoke_result_t<F, expected<T>&>> {
    return captured_invoke(std::forward<F>(f), self());
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, expected<T> const&>, int> = 0>
  auto map(F&& f) const& noexcept
      -> expected<std::invoke_result_t<F, expected<T> const&>> {
    return captured_invoke(std::forward<F>(f), self());
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, expected<T>&&>, int> = 0>
  auto map(F&& f) && noexcept -> expected<std::invoke_result_t<F, expected<T>&&>> {
    return captured_invoke(std::forward<F>(f), std::move(self()));
  }

  void rethrow_error() const {
    if (self().has_error()) {
      std::rethrow_exception(self().error());
    }
  }
  template <typename E, typename F, std::enable_if_t<std::is_invocable_v<F, E const&>, int> = 0>
  auto catch_error(F&& f) -> std::optional<std::invoke_result_t<F, E const&>> {
    try {
      self().rethrow_error();
    } catch (E const& e) {
      return std::invoke(std::forward<F>(f), e);
    } catch (...) {
    }
    return std::nullopt;
  }

  explicit operator bool() const noexcept { return self().has_error(); }

 private:
  expected<T>& self() & { return static_cast<expected<T>*>(this); }
  expected<T> const& self() const& {
    return *static_cast<expected<T> const*>(this);
  }
};

}  // namespace detail

template <typename T>
struct expected : detail::expected_base<T> {
  static_assert(!std::is_void_v<T> && !std::is_reference_v<T>);

  expected() = delete;

  expected(std::exception_ptr p)
      : _exception(std::move(p)), _has_value(false) {}
  expected(T t) : _value(std::move(t)), _has_value(true) {}

  template <typename... Ts, std::enable_if_t<std::is_constructible_v<T, Ts...>, int> = 0>
  expected(std::in_place_t, Ts&&... ts)
      : _value(std::forward<Ts>(ts)...), _has_value(true) {}

  template <typename U = T, std::enable_if_t<std::is_move_constructible_v<U>, int> = 0>
  expected(expected&& o) noexcept(std::is_nothrow_move_constructible_v<U>)
      : _has_value(o._has_value) {
    if (o._has_value) {
      new (&this->_value) T(std::move(o._value));
    } else {
      new (&this->_exception) std::exception_ptr(std::move(o._exception));
    }
  }

  ~expected() {
    if (_has_value) {
      _value.~T();
    } else {
      _exception.~exception_ptr();
    }
  }

  expected(expected const&) = delete;
  expected(expected&&) noexcept = default;

  expected& operator=(expected const&) = delete;
  expected& operator=(expected&&) noexcept = default;

  T& get() & {
    if (_has_value) {
      return _value;
    }
    std::rethrow_exception(_exception);
  }
  T const& get() const& {
    if (_has_value) {
      return _value;
    }
    std::rethrow_exception(_exception);
  }
  T&& get() && {
    if (_has_value) {
      return std::move(_value);
    }
    std::rethrow_exception(_exception);
  }

  [[nodiscard]] std::exception_ptr error() const {
    if (has_error()) {
      return _exception;
    }
    return nullptr;
  }

  T* operator->() { return &get(); }
  T const* operator->() const { return &get(); }

  [[nodiscard]] bool has_value() const noexcept { return _has_value; }
  [[nodiscard]] bool has_error() const noexcept { return !has_value(); }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T&>, int> = 0>
  auto map_value(F&& f) & noexcept -> expected<std::invoke_result_t<F, T&>> {
    if (has_error()) {
      return _exception;
    }

    return captured_invoke(std::forward<F>(f), _value);
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T const&>, int> = 0>
  auto map_value(F&& f) const& noexcept
      -> expected<std::invoke_result_t<F, T const&>> {
    if (has_error()) {
      return _exception;
    }

    return captured_invoke(std::forward<F>(f), _value);
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T&&>, int> = 0>
  auto map_value(F&& f) && noexcept -> expected<std::invoke_result_t<F, T&&>> {
    if (has_error()) {
      return std::move(_exception);
    }

    return captured_invoke(std::forward<F>(f), std::move(_value));
  }

 private:
  union {
    T _value;
    std::exception_ptr _exception;
  };
  const bool _has_value;
};

template <>
struct expected<void> : detail::expected_base<void> {
  expected() = default;
  expected(std::exception_ptr p) : _exception(std::move(p)) {}
  expected(std::in_place_t) : _exception(nullptr) {}
  ~expected() = default;

  expected(expected const&) = delete;
  expected(expected&&) noexcept = default;

  expected& operator=(expected const&) = delete;
  expected& operator=(expected&&) noexcept = default;

  [[nodiscard]] bool has_error() const noexcept {
    return _exception != nullptr;
  }
  [[nodiscard]] std::exception_ptr error() const {
    if (has_error()) {
      return _exception;
    }
    return nullptr;
  }

 private:
  std::exception_ptr _exception = nullptr;
};

template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int>>
auto captured_invoke(F&& f, Args&&... args) noexcept
    -> expected<std::invoke_result_t<F, Args...>> {
  try {
    return std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
  } catch (...) {
    return std::current_exception();
  }
}

}  // namespace expect

#endif  // PROTOTYPE_EXPECTED_H
