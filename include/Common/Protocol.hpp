#pragma once

#include <cstddef>
#include <memory_resource>
#include <string>
#include <string_view>

#include "Common/Command.hpp"
#include "Common/Result.hpp"

namespace KV
{
enum class DecodeStatus
{
    kComplete,
    kIncomplete,
    kProtocolError,
};

struct RequestDecode
{
    DecodeStatus status = DecodeStatus::kIncomplete;
    std::size_t consumed_bytes = 0;
    std::pmr::string error;
    Command command;
};

struct ResponseDecode
{
    DecodeStatus status = DecodeStatus::kIncomplete;
    std::size_t consumed_bytes = 0;
    std::pmr::string error;
    Result result;
};

class Protocol
{
public:
    explicit Protocol(std::pmr::memory_resource* resource = std::pmr::get_default_resource()) noexcept;

    std::pmr::string EncodeRequest(const Command& command) const;
    RequestDecode DecodeRequest(std::string_view input) const;

    std::pmr::string EncodeResponse(const Result& result) const;
    ResponseDecode DecodeResponse(std::string_view input) const;

private:
    std::pmr::memory_resource* resource_;
};
} // namespace KV
