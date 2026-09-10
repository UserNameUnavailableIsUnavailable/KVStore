#pragma once

#include <Foundation/Async/Session.hpp>
#include <Foundation/Buffer.hpp>
#include <Foundation/Socket.hpp>

#include <Application/RESP/RESP.hpp>

namespace RESP
{
class Sender
{
  public:
    Sender(Foundation::Async::Session &session, ::Foundation::Buffer &buffer, const Object &object);
    ~Sender() noexcept;

    Foundation::Async::Task<bool> send();
    std::size_t bytes_sent() const noexcept
    {
        return bytes_sent_;
    }

    std::string encode_error() const
    {
        return encode_error_;
    }

    std::string internal_error() const
    {
        return internal_error_;
    }

  private:
    Foundation::Async::Session &session_;
    Foundation::Buffer &buffer_;
    const Object &object_;
    std::size_t bytes_sent_{0};
    std::string encode_error_;
    std::string internal_error_;
};

} // namespace RESP