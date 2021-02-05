#ifndef PROTOTYPE_HELPER_H
#define PROTOTYPE_HELPER_H

namespace futures {
inline namespace v2 {
namespace detail {
template <typename T>
using pointer_to_t = T*;

struct unit_type {};
template <typename T>
using devoidify_t = std::conditional_t<std::is_void_v<T>, unit_type, T>;

template<typename T>
struct non_deleter {
  void operator()(T *) const noexcept  {}
};

template<typename T>
using moving_ptr = std::unique_ptr<T, non_deleter<T>>;

}
}
}

#endif  // PROTOTYPE_HELPER_H
