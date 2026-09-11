#pragma once

#include <Foundation/File.hpp>

namespace Foundation::Async
{

using ReadStatus = ::Foundation::ReadStatus;
using ReadResult = ::Foundation::ReadResult;
using WriteStatus = ::Foundation::WriteStatus;
using WriteResult = ::Foundation::WriteResult;

using FileReadStatus = ReadStatus;
using FileReadResult = ReadResult;
using FileWriteStatus = WriteStatus;
using FileWriteResult = WriteResult;

} // namespace Foundation::Async
