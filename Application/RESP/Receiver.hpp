#pragma once

#include <cstddef>
#include <Foundation/NBIO/Runtime.hpp>
#include <optional>

#include <Foundation/NBIO/Session.hpp>
#include <Foundation/Async/Task.hpp>

#include <Application/RESP/RESP.hpp>

namespace RESP
{
class Receiver
{
  public:
    Receiver(Foundation::NBIO::Session &session, ::Foundation::Core::Buffer &buffer);
    ~Receiver() noexcept;

    Foundation::NBIO::Task<std::optional<Object>> receive();
    std::size_t bytes_received() const noexcept
    {
        return bytes_received_;
    }
    std::string decode_error() const
    {
        return decode_error_;
    }
    std::string internal_error() const
    {
        return internal_error_;
    }

  private:
    Foundation::NBIO::Session &session_;
    Foundation::Core::Buffer &buffer_;
    std::size_t bytes_received_{0};
    std::string decode_error_;
    std::string internal_error_;
};
} // namespace RESP
