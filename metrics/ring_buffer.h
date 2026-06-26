#ifndef RING_BUFFER_H
#define RING_BUFFER_H

#include <array>
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <mutex>

template<typename T, size_t Capacity>
class RingBuffer {
public:
    RingBuffer() : head_(0), size_(0) {}

    void push(const T& item) {
        std::lock_guard<std::mutex> lock(mutex_);
        data_[head_] = item;
        head_ = (head_ + 1) % Capacity;
        if (size_ < Capacity) {
            ++size_;
        }
    }

    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_;
    }

    bool empty() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return size_ == 0;
    }

    size_t capacity() const {
        return Capacity;
    }

    void clear() {
        std::lock_guard<std::mutex> lock(mutex_);
        head_ = 0;
        size_ = 0;
    }

    template<typename Func>
    void forEach(Func&& func) const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == 0) return;
        size_t start = (head_ + Capacity - size_) % Capacity;
        for (size_t i = 0; i < size_; ++i) {
            func(data_[(start + i) % Capacity]);
        }
    }

    T latest() const {
        std::lock_guard<std::mutex> lock(mutex_);
        if (size_ == 0) return T{};
        return data_[(head_ + Capacity - 1) % Capacity];
    }

private:
    mutable std::mutex mutex_;
    std::array<T, Capacity> data_;
    size_t head_;
    size_t size_;
};

#endif
