#pragma once

#include <utility>

namespace nbio::utility {
template <typename F>
class Defer {
   public:
    explicit Defer(F&& func) : func_(std::forward<F>(func)) {}
    ~Defer() { func_(); }

   private:
    F func_;
};
template <typename F>
Defer<F> MakeDefer(F&& func) {
    return Defer<F>(std::forward<F>(func));
}
}  // namespace nbio::utility

