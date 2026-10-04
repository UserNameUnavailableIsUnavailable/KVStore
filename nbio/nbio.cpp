#include <nbio/nbio.hpp>

#include <nbio/runtime/Runtime.hpp>

namespace nbio {
void initialize(std::unique_ptr<core::Multiplexer> multiplexer) { nbio::runtime::initialize(std::move(multiplexer)); }

void run(Task<void> main) { nbio::async::run(std::move(main)); }
}  // namespace nbio

