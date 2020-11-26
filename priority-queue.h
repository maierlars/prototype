#ifndef PROTOTYPE_PRIORITY_QUEUE_H
#define PROTOTYPE_PRIORITY_QUEUE_H

#include <algorithm>
#include <vector>

namespace foobar {

template<typename T>
class priority_queue {
 public:
  [[nodiscard]] auto find_max() -> T const&;

  [[nodiscard]] auto extract_max() -> T;

  template<typename... Args>
  auto emplace(Args&&... args) -> void;

  [[nodiscard]] auto empty() const -> bool;

 private:
  std::vector<T> _heap;
};

template<typename T>
auto priority_queue<T>::empty() const -> bool {
  return _heap.empty();
}

template<typename T>
template<typename... Args>
auto priority_queue<T>::emplace(Args &&... args) -> void {
  _heap.emplace_back(std::forward<Args>(args)...);
  std::push_heap(_heap.begin(), _heap.end());
}

template<typename T>
auto priority_queue<T>::find_max() -> T const & {
  return _heap.front();
}

template<typename T>
auto priority_queue<T>::extract_max() -> T {
  std::pop_heap(_heap.begin(), _heap.end());
  auto value = std::move(_heap.back());
  _heap.pop_back();

  return value;
}

}

#endif  // PROTOTYPE_PRIORITY_QUEUE_H
