#ifndef TRDPOOL_H__
#define TRDPOOL_H__

#include <functional>
#include <future>
#include <memory>
#include <type_traits>
#include <utility>
#include <chrono>

#include "threadpool.h"

class TrdPool {
public:
    template<typename T>
    struct TaskWrapper {
        std::function<T()> func;
        std::promise<T> promise;

        TaskWrapper(std::function<T()>&& f) : func(std::move(f)) {}
    };

    template<typename T>
    static void* execute_task(void* param) {
        auto wrapper = static_cast<TaskWrapper<T>*>(param);
        try {
            if constexpr (std::is_void_v<T>) {
                wrapper->func();
                wrapper->promise.set_value();
            } else {
                auto result = wrapper->func();
                wrapper->promise.set_value(std::move(result));
            }
        } catch (...) {
            wrapper->promise.set_exception(std::current_exception());
        }
        delete wrapper;
        return nullptr;
    }

private:
    struct thrdpool* pool_;

public:
    explicit TrdPool(size_t thread_count = std::thread::hardware_concurrency())
        : pool_(creatpool(thread_count)) {
        if (!pool_) {
            throw std::runtime_error("Failed to create thread pool");
        }
    }

    ~TrdPool() {
        if (pool_) {
            setpoolblock(pool_, false);
            destroypool(pool_);
        }
    }

    TrdPool(const TrdPool&) = delete;
    TrdPool& operator=(const TrdPool&) = delete;

    TrdPool(TrdPool&& other) noexcept : pool_(other.pool_) {
        other.pool_ = nullptr;
    }

    TrdPool& operator=(TrdPool&& other) noexcept {
        if (this != &other) {
            if (pool_) {
                destroypool(pool_);
            }
            pool_ = other.pool_;
            other.pool_ = nullptr;
        }
        return *this;
    }

    template<typename F, typename... Args>
    auto submit(F&& func, Args&&... args) -> std::future<std::invoke_result_t<F, Args...>> {
        using ReturnType = std::invoke_result_t<F, Args...>;

        auto bound_func = std::bind(std::forward<F>(func), std::forward<Args>(args)...);
        auto wrapper = new TaskWrapper<ReturnType>(std::move(bound_func));
        auto future = wrapper->promise.get_future();

        task_id id = addtask(pool_, execute_task<ReturnType>, wrapper, 0);
        if (id == 0) {
            delete wrapper;
            throw std::runtime_error("Failed to add task to thread pool");
        }

        return future;
    }

    void submit_fire_and_forget(std::function<void()> func) {
        auto wrapper = new TaskWrapper<void>(std::move(func));

        task_id id = addtask(pool_, execute_task<void>, wrapper, 0);
        if (id == 0) {
            delete wrapper;
            throw std::runtime_error("Failed to add task to thread pool");
        }
    }

    size_t tasks_in_queue() const {
        return pool_ ? taskinqueu(pool_) : 0;
    }

    bool setblock(bool block) {
        return setpoolblock(pool_, block);
    }

    void wait_all() {
        if (pool_) {
            void* result;
            waittask(pool_, 0, &result);
        }
    }
};

inline void submit_delay_job(std::function<void()> func, unsigned int delay_sec) {
    using TaskWrapper = TrdPool::TaskWrapper<void>;
    auto wrapper = new TaskWrapper(std::move(func));
    if(!add_delay_job(TrdPool::execute_task<void>, wrapper, delay_sec)) {
        delete wrapper;
    }
}

#endif
