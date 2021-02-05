#ifndef PROTOTYPE_DESERIALIZE_H
#define PROTOTYPE_DESERIALIZE_H

#include <velocypack/Slice.h>
#include <velocypack/velocypack-aliases.h>

template<typename T>
struct deserializer;

template<typename T>
T deserialize(VPackSlice s) {
  static_assert(std::is_invocable_r_v<T, deserializer<T>, VPackSlice>);
  return deserializer<T>{}(s);
}

template<typename T>
struct serializer;

template<typename T>
T serialize(VPackSlice s) {
  static_assert(std::is_invocable_r_v<T, serializer<T>, VPackSlice>);
  return serializer<T>{}(s);
}
#endif  // PROTOTYPE_DESERIALIZE_H
