#include <gtest/gtest.h>
#include <coroutine>

// The caller-level type
struct Generator {
  // The coroutine level type
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;
    Generator get_return_object() { return Generator{Handle::from_promise(*this)}; }
    std::suspend_always initial_suspend() { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    std::suspend_always yield_value(int value) {
      current_value = value;
      return {};
    }
    void unhandled_exception() {}
    int current_value;
  };

  constexpr bool await_ready() const noexcept { return false; }
  void await_suspend(std::coroutine_handle<> h) const {}
  constexpr void await_resume() const noexcept {}
  explicit Generator(promise_type::Handle coro) : coro_(coro) {}

  ~Generator() {
    if (coro_) coro_.destroy();
  }
  // Make move-only
  Generator(const Generator&) = delete;
  Generator& operator=(const Generator&) = delete;
  Generator(Generator&& t) noexcept : coro_(t.coro_) { t.coro_ = {}; }
  Generator& operator=(Generator&& t) noexcept {
    if (this == &t) return *this;
    if (coro_) coro_.destroy();
    coro_ = t.coro_;
    t.coro_ = {};
    return *this;
  }
  int get_next() {
    coro_.resume();
    return coro_.promise().current_value;
  }

 private:
  promise_type::Handle coro_;
};

Generator myCoroutine() {
  int x = 0;
  while (true) {
    co_yield x++;
    co_await myCoroutine();
  }
}

TEST(CoroTest, Simple) {
  auto c = myCoroutine();
  int x = 0;
  while ((x = c.get_next()) < 10) {
    std::cout << x << "\n";
  }
  auto d = myCoroutine();
  int y = 0;
  while ((y = d.get_next()) < 10) {
    std::cout << y << "\n";
  }
}

TEST(CoroTest, ContainerOfCoro) {
  std::vector<Generator> coros;
  for (int i = 0; i < 10; ++i) {
    coros.emplace_back(myCoroutine());
  }
  for (auto& c : coros) {
    int x = 0;
    while ((x = c.get_next()) < 10) {
      std::cout << x << "\n";
    }
  }
}
