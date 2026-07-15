#pragma once

#include <string>

namespace KV
{

enum class Command
{
	kExists,
	kGet,
	kSet,
	kDelete,
	kUpdate
};

enum class Status
{
	kPending,
	kSuccess,
	kError
};

} // KV
