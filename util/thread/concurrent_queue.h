#include <condition_variable>
#include <chrono>
#include <mutex>
#include <queue>
#include <thread>

template <typename T> class WaitQueue {
public:
  T pop() {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) {
      cond_.wait(mlock);
    }
    auto item = queue_.front();
    queue_.pop();
    return item;
  }

  void pop(T &item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    while (queue_.empty()) {
      cond_.wait(mlock);
    }
    item = queue_.front();
    queue_.pop();
  }

  void pop(T &item, const std::chrono::milliseconds& timeout, bool *timed_out) {
    *timed_out = false;
    std::unique_lock<std::mutex> mlock(mutex_);
    if (queue_.empty()) {
        if (cond_.wait_for(mlock, timeout) == std::cv_status::timeout || queue_.empty()) {
            *timed_out = true;
            return;
        }
    }
    while (queue_.empty()) {
      cond_.wait(mlock);
    }
    item = queue_.front();
    queue_.pop();
  }

  void push(const T &item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(item);
    mlock.unlock();
    cond_.notify_one();
  }

  void push(T &&item) {
    std::unique_lock<std::mutex> mlock(mutex_);
    queue_.push(std::move(item));
    mlock.unlock();
    cond_.notify_one();
  }

private:
  std::queue<T> queue_;
  std::mutex mutex_;
  std::condition_variable cond_;
};