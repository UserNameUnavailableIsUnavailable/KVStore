#pragma once

#include <memory>
#include <string>
#include <vector>

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

// We allocate a fix-sized request pool, so that we don't have to create
// Request instances frequently.

class Request
{
public:
	void clear()
	{
		tokens_.clear();
	}
private:
	Request();
	std::vector<std::string> tokens_;
};

const Request* MakeRequest(std::string s);

class Response
{
private:
	std::string message_;
};

} // KV
