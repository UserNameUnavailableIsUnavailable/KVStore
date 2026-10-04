#include <Foundation/NBIO/NBIO.hpp>

#include "Engine.hpp"

namespace Foundation::NBIO {
void initialize(std::unique_ptr<Multiplexer> multiplexer) { Engine::initialize(std::move(multiplexer)); }

void run(Task<void> main) { Foundation::Async::run(std::move(main)); }
}  // namespace Foundation::NBIO
