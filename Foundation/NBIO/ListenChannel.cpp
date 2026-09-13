#include "ListenChannel.hpp"
#include <Foundation/Async/Coroutine.hpp>
#include <Foundation/NBIO/Runtime.hpp>

#include <Foundation/NBIO/Multiplexer.hpp>
#include <Foundation/Core/Socket.hpp>
#include <cassert>
#include <stdexcept>
#include <utility>

#include <Foundation/Async/Scheduler.hpp>

namespace Foundation::NBIO
{
ListenChannel::ListenChannel(Foundation::Core::Socket socket, Foundation::NBIO::Multiplexer &multiplexer, Foundation::Async::Scheduler &scheduler)
    : Foundation::NBIO::Channel(Foundation::NBIO::ChannelType::kListen, socket.native_handle(), multiplexer, scheduler),
      socket_(std::move(socket))
{
    if (!socket_.is_valid())
    {
        throw std::logic_error("invalid socket");
    }
    socket_.set_non_blocking(true);
    multiplexer_.add_channel(this);
}

ListenChannel::~ListenChannel() noexcept
{
    multiplexer_.delete_channel(this);
}

namespace
{
class AcceptAwaiter
{
  public:
    AcceptAwaiter(ListenChannel &channel) : channel_(channel)
    {
    }

    AcceptAwaiter(const AcceptAwaiter &) = delete;
    AcceptAwaiter &operator=(const AcceptAwaiter &) = delete;

    ~AcceptAwaiter() noexcept = default;

    bool await_ready() const noexcept
    {
        return false;
    }
    template <typename PromiseType>
    void await_suspend(std::coroutine_handle<PromiseType> handle)
    {
        coroutine_ = Async::Coroutine::from_handle(handle);
        channel_.arm();
        channel_.park(coroutine_);
        channel_.job() =
            AcceptJob{.result = {.status = Foundation::Core::AcceptStatus::kPending, .socket = {}, .address = {}, .error_code = {}}};
    }

    Foundation::Core::AcceptResult await_resume() noexcept
    {
        AcceptJob &job = channel_.job();
        assert(job.result.status != Foundation::Core::AcceptStatus::kPending);
        return std::move(job.result);
    }

  private:
    ListenChannel &channel_;
    Async::Coroutine coroutine_;
};
} // namespace

void ListenChannel::handle_event()
{
    // handle the triggered event
    if (handler_ != nullptr) [[likely]]
    {
        handler_(this);
    }

    if (!waiter_) [[unlikely]]
    {
        return;
    }

    if (job().result.status == Foundation::Core::AcceptStatus::kPending)
    {
        // No connection was ready (EAGAIN): stay armed, keep suspended.
        arm();
        return;
    }

    // waiter_ is valid AND job finished in success or error
    scheduler_.submit(std::exchange(waiter_, {}));
}

Foundation::NBIO::Task<Foundation::Core::AcceptResult> ListenChannel::accept()
{
    auto result = co_await AcceptAwaiter(*this);
    co_return std::move(result);
}
} // namespace Foundation::NBIO
