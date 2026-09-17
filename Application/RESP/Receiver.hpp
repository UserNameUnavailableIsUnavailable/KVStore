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

    std::string decode_error() const
    {
        return decode_error_;
    }
    std::string internal_error() const
    {
        return interal_error_;
    }

  private:
    Foundation::NBIO::Session &session_;
    Foundation::Core::Buffer &buffer_;
    std::string decode_error_;
    std::string interal_error_;
};
} // namespace RESP
