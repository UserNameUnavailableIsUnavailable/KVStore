#pragma once

#include <Foundation/NBIO/Session.hpp>
#include <Foundation/NBIO/Runtime.hpp>
#include <Foundation/Core/Buffer.hpp>
#include <Foundation/Core/Socket.hpp>

#include <Application/RESP/RESP.hpp>

namespace RESP
{
class Sender
{
  public:
    Sender(Foundation::NBIO::Session &session, ::Foundation::Core::Buffer &buffer, const Object &object);
    ~Sender() noexcept;

    Foundation::NBIO::Task<bool> send();
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
    Foundation::NBIO::Session &session_;
    Foundation::Core::Buffer &buffer_;
    const Object &object_;
    std::size_t bytes_sent_{0};
    std::string encode_error_;
    std::string internal_error_;
};

} // namespace RESP
