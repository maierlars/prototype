#ifndef FUTURES_BOX_H
#define FUTURES_BOX_H

namespace futures {
inline namespace v2 {
namespace detail {

template <typename T>
struct box {
  box() = default;
  template <typename... Args>
  explicit box(std::in_place_t, Args&&... args) {
    new (ptr()) T(std::forward<Args>(args)...);
  }

  T* ptr() noexcept { return reinterpret_cast<T*>(_store.data()); }
  T const* ptr() const noexcept { return reinterpret_cast<T*>(_store.data()); }

  T& ref() & noexcept { return *ptr(); }
  T const& ref() const& noexcept { return *ptr(); }
  T&& ref() && noexcept { return std::move(ref()); }
  T&& move() noexcept { return std::move(ref()); }

  template <typename... Args>
  void emplace(Args&&... args) noexcept(std::is_nothrow_constructible_v<T, Args...>) {
    static_assert(std::is_constructible_v<T, Args...>);
    new (ptr()) T(std::forward<Args>(args)...);
  }

  template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
            typename R = std::invoke_result_t<F, Args...>,
            std::enable_if_t<std::is_constructible_v<T, R>, int> = 0>
  void emplace_result(F&& f, Args&&... args) noexcept(
      std::is_nothrow_invocable_v<F>&& std::is_nothrow_constructible_v<T, R>) {
    new (ptr()) T(std::invoke(std::forward<F>(f), std::forward<Args>(args)...));
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F, T&&>, int> = 0,
            typename R = std::invoke_result_t<F, T&&>>
  R move_into(F&& f) noexcept(std::is_nothrow_invocable_v<F, T&&>) {
    return std::invoke(std::forward<F>(f), this->move());
  }

  void destroy() noexcept(std::is_nothrow_destructible_v<T>) { ref().~T(); }

 private:
  alignas(T) std::array<std::byte, sizeof(T)> _store;
};

template <>
struct box<void> {
  box() = default;
  explicit box(std::in_place_t) {}

  void emplace() noexcept {}
  template <typename F, typename... Args, std::enable_if_t<std::is_invocable_r_v<void, F, Args...>, int> = 0>
  void emplace_result(F&& f, Args&&... args) const
      noexcept(std::is_nothrow_invocable_v<F>) {
    std::invoke(std::forward<F>(f), std::forward<Args>(args)...);
  }

  template <typename F, std::enable_if_t<std::is_invocable_v<F>, int> = 0, typename R = std::invoke_result_t<F>>
  R move_into(F&& f) noexcept(std::is_nothrow_invocable_v<F>) {
    return std::invoke(std::forward<F>(f));
  }

  void destroy() const noexcept {}
};

template <typename T, typename = void>
struct small_box {
  static constexpr auto has_value = false;
};

template <typename T>
struct small_box<T, std::enable_if_t<sizeof(T) < 64>> : box<T> {
  static constexpr auto has_value = true;
};

}  // namespace detail
}  // namespace v2
}  // namespace futures

#endif  // FUTURES_BOX_H
