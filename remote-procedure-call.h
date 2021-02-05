#ifndef PROTOTYPE_REMOTE_PROCEDURE_CALL_H
#define PROTOTYPE_REMOTE_PROCEDURE_CALL_H
#include <utility>
#include <functional>
#include <memory>

template<typename T>
struct future {};


template<const char N[], auto Ptr>
struct remote_procedure;

template<const char N[], typename T, template<typename> typename Base, typename Req, typename Resp, Resp(Base<T>::*Proc)(Req)>
struct remote_procedure<N, Proc> {
  using request_type = Req;
  using response_type = Resp;
  using base_type = Base;
  static inline constexpr auto name = N;

  template<typename B>
  future<response_type> operator()(B && base, request_type req) {
    std::invoke(Proc, std::forward<B>(base), req);
  }
};

template<typename...>
struct remote_procedure_list;

template<const char... N[], auto Proc>
struct remote_procedure_list<remote_procedure<N, Proc>...> {};

template<typename T>
using identity_t = T;


struct communicator {};

struct foo_bar_service {
  struct FooRequest {};
  struct FooResponse {};

  virtual FooResponse foo(FooRequest) = 0;

  struct BarRequest {};
  struct BarResponse {};

  virtual BarResponse bar(BarRequest) = 0;
};

static constexpr char foo_name[] = "foo";

using remote_procedures = remote_procedure_list<
    remote_procedure<foo_name, &foo_bar_service::foo>
>;

struct foo_bar_service_impl : foo_bar_service<> {
  virtual ~foo_bar_service_impl();
  int foo(FooRequest) override;
  BarResponse bar(BarRequest) override;
};


struct foo_bar_service_remote : foo_bar_service<future> {
  virtual ~foo_bar_service_remote();
  future<int> foo(FooRequest) override {

  }


  future<BarResponse> bar(BarRequest) override;

  std::shared_ptr<communicator> comm;
};

#endif  // PROTOTYPE_REMOTE_PROCEDURE_CALL_H
