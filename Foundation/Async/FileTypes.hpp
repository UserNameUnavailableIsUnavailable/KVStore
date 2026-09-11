#pragma once

#include <Foundation/Core/File.hpp>

namespace Foundation::Async
{

using ReadStatus = ::Foundation::Core::ReadStatus;
using ReadResult = ::Foundation::Core::ReadResult;
using WriteStatus = ::Foundation::Core::WriteStatus;
using WriteResult = ::Foundation::Core::WriteResult;

using FileReadStatus = ReadStatus;
using FileReadResult = ReadResult;
using FileWriteStatus = WriteStatus;
using FileWriteResult = WriteResult;

} // namespace Foundation::Async
