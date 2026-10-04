#include <NBIO/NBIO.hpp>

#include <NBIO/Runtime/Runtime.hpp>

namespace NBIO {
void initialize(std::unique_ptr<Core::Multiplexer> multiplexer) { NBIO::Runtime::initialize(std::move(multiplexer)); }

void run(Task<void> main) { NBIO::Async::run(std::move(main)); }
}  // namespace NBIO

