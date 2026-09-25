#include <Foundation/NBIO/NBIO.hpp>

#include "Engine.hpp"

namespace Foundation::NBIO
{
void initialize(std::unique_ptr<Multiplexer> multiplexer)
{
    Engine::initialize(std::move(multiplexer));
}

void run(Task<void> main)
{
    Foundation::Async::run(std::move(main));
}

std::shared_ptr<FileStream> open_file(const std::filesystem::path &p)
{
    return FileStream::Open(p.string(), Foundation::Core::FileMode::kReadWrite | Foundation::Core::FileMode::kCreate, 0644,
                            Engine::multiplexer(), Engine::scheduler());
}
} // namespace Foundation::NBIO
