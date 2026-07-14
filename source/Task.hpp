#pragma once

#include <string>

namespace KV
{

enum class TaskType
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

};
