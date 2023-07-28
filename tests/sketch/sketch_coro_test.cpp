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

struct CoroFoo {
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;
    CoroFoo get_return_object() { return CoroFoo{Handle::from_promise(*this)}; }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    std::suspend_always yield_value(int value) noexcept {
      value_ = value;
      return {};
    }
    void return_value(int value) noexcept { value_ = value; }
    void unhandled_exception() {}
    int value_;
  };

  CoroFoo(typename promise_type::Handle handle) : handle_(handle) {}
  CoroFoo(const CoroFoo&) = delete;
  CoroFoo(CoroFoo&& coro) : handle_(coro.handle_) { coro.handle_ = nullptr; }
  ~CoroFoo() {
    if (handle_) {
      handle_.destroy();
    }
  }

  int Run() {
    handle_.resume();
    return handle_.promise().value_;
  }

  typename promise_type::Handle handle_;
};

struct CoroBar {
  struct promise_type {
    using Handle = std::coroutine_handle<promise_type>;
    CoroBar get_return_object() { return CoroBar{Handle::from_promise(*this)}; }
    std::suspend_always initial_suspend() noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; }
    std::suspend_always yield_value(int value) noexcept {
      value_ = value;
      return {};
    }
    void return_value(int value) noexcept { value_ = value; }
    void unhandled_exception() {}
    int value_;
  };

  constexpr bool await_ready() const noexcept { return false; }
  constexpr bool await_suspend(std::coroutine_handle<>) const noexcept { return true; }
  int await_resume() const noexcept { return handle_.promise().value_; }

  CoroBar(typename promise_type::Handle handle) : handle_(handle) {}
  CoroBar(const CoroBar&) = delete;
  CoroBar(CoroBar&& coro) : handle_(coro.handle_) { coro.handle_ = nullptr; }
  ~CoroBar() {
    if (handle_) {
      handle_.destroy();
    }
  }

  int Run() {
    handle_.resume();
    return handle_.promise().value_;
  }

  typename promise_type::Handle handle_;
};

CoroBar Bar(int n) {
  co_yield n;
  co_yield n + 1;
  co_return n + 2;
}

CoroFoo Foo(int n) {
  int i = 0;
  while (i < n) {
    auto bar = Bar(i);
    while (!bar.handle_.done()) {
      auto temp = bar.Run();
      co_yield temp;
    }
    i += 3;
  }
  co_return i;
}

auto foo = Foo(10);

void Func() { std::cout << foo.Run() << std::endl; }

TEST(CoroTest, YieldInAwait) {
  while (!foo.handle_.done()) {
    Func();
  }
}
