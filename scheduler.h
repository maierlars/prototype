#ifndef PROTOTYPE_SCHEDULER_H
#define PROTOTYPE_SCHEDULER_H
#include <chrono>
#include <deque>
#include <mutex>
#include <tuple>
#include <utility>

#include "futures.h"

namespace sched {

struct scheduler {
  template <typename F, typename... Args, std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
            typename S = std::invoke_result_t<F, Args...>, typename R = futures::detail::future_base_type_t<S>>
  auto async(F&& f, Args&&... args) -> futures::future<R> {
    auto&& [ff, p] = futures::make_promise<R>();

    post([p = std::move(p), f = std::forward<F>(f),
          args = std::make_tuple(std::forward<Args>(args)...)]() mutable noexcept {
      std::move(p).capture([&] { return std::apply(f, args); });
    });

    return std::move(ff);
  }

  template <typename F, typename... Args,
            std::enable_if_t<std::is_nothrow_invocable_r_v<void, F, Args...>, int> = 0>
  void post(F&& f, Args&&... args) {
    std::unique_lock guard(mutex);

    if constexpr (sizeof...(Args) == 0) {
      queue.emplace_back(std::in_place, std::forward<F>(f));
    } else {
      queue.emplace_back(std::in_place,
                         [f = std::forward<F>(f),
                          args = std::make_tuple(std::forward<Args>(args)...)]() noexcept {
                           std::apply(f, args);
                         });
    }

    if (sleeping > 0) {
      cv.notify_one();
    }
  }

  using clock = std::chrono::steady_clock;

  template <typename Rep, typename Period, typename F, typename... Args,
            std::enable_if_t<std::is_invocable_v<F, Args...>, int> = 0,
            typename R = std::invoke_result_t<F, Args...>>
  auto delay(std::chrono::duration<Rep, Period> const& d, F&& f, Args&&... args)
      -> futures::future<R> {
    auto&& [ff, p] = futures::make_promise<R>();

    auto start_at = clock::now() + std::chrono::duration_cast<clock::duration>(d);

    delayed_task_queue.emplace(start_at, std::in_place,
                               [p = std::move(p), f = std::forward<F>(f),
                                args = std::make_tuple(std::forward<Args>(args)...)]() mutable noexcept {
                                 std::move(p).capture(
                                     [&] { return std::apply(f, args); });
                               });

    return std::move(ff);
  }

  void run() {
    std::unique_lock guard(mutex);
    while (!is_stopping) {
      if (!queue.empty()) {
        auto task = std::move(queue.front());
        queue.pop_front();

        guard.unlock();
        task.operator()();
        guard.lock();
        continue;
      }

      sleeping += 1;
      cv.wait(guard);
      sleeping -= 1;
    }
  }

  void stop() {
    {
      std::unique_lock guard(mutex);
      is_stopping = true;
    }
    cv.notify_all();
  }

 private:
  struct task_base {
    virtual ~task_base() = default;
    virtual void operator()() noexcept = 0;
  };

  template <typename F>
  struct task_lambda final : task_base {
    template <typename G = F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, G>, int> = 0>
    task_lambda(std::in_place_t, F&& f) : f(std::forward<F>(f)) {}
    void operator()() noexcept final { f.operator()(); }

   private:
    F f;
  };

  struct task {
    template <typename F, std::enable_if_t<std::is_nothrow_invocable_r_v<void, F>, int> = 0>
    task(std::in_place_t, F&& f)
        : ptr(std::make_unique<task_lambda<F>>(std::in_place, std::forward<F>(f))) {}

    void operator()() noexcept { ptr->operator()(); }

   private:
    std::unique_ptr<task_base> ptr;
  };

  struct delayed_task : task {
    template <typename... Args>
    explicit delayed_task(clock::time_point start_at, Args&&... args)
        : task(std::forward<Args>(args)...), start_at(start_at) {}

    bool operator<(delayed_task const& o) const noexcept {
      return start_at > o.start_at;  // max heap
    }

   private:
    clock::time_point start_at;
  };

  std::atomic<bool> is_stopping = false;
  std::mutex mutex;
  std::deque<task> queue;
  std::condition_variable cv;
  std::size_t sleeping = 0;
  foobar::priority_queue<delayed_task> delayed_task_queue;
};

}  // namespace sched

#endif  // PROTOTYPE_SCHEDULER_H
