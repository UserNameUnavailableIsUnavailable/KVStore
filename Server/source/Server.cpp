#include "Server/Server.hpp"

#include <format>
#include <iostream>
#include <optional>
#include <sstream>
#include "Common/LRUCache.hpp"

#include <liburing/io_uring.h>

namespace KV
{

Server::Server() :
	lru_cache_(32)
{
    server_fd_ = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
    {
        throw std::runtime_error(std::format("failed to create socket, errno: {}", errno));
    }

    int yes = 1;
    if (setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        throw std::runtime_error(std::format("failed to set SO_REUSEADDR, errno: {}", errno));
    }

	RegisterHandlers();
}

void Server::Bind(std::uint16_t port)
{
    sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {
            .s_addr = htonl(INADDR_ANY),
        },
        .sin_zero = {}
    };
    int ret = bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0)
    {
        throw std::runtime_error((std::format("failed to bind to port {}, errno: {}", port, errno)));
    }
    port_ = port;
    bound_ = true;
}

bool Server::Prepare(Task& task)
{
	auto sqe_ptr = io_uring_get_sqe(&ring_);
	if (!sqe_ptr)
	{
		std::size_t index = std::distance(inprogress_.begin(), &task);
		defer_.push_back(index);
		return false;
	}
	sqe_ptr->user_data = reinterpret_cast<std::uint64_t>(&task);
	switch (task.type)
	{
		case TaskType::kAccept:
		{
			io_uring_prep_accept(sqe_ptr, server_fd_, reinterpret_cast<sockaddr*>(&task.address), &task.address_length, 0);
			break;
		}
		case TaskType::kRead:
		{
			io_uring_prep_read(sqe_ptr, task.fd, task.read_buffer.data(), task.read_buffer.size(), 0);
			break;
		}
		case TaskType::kWrite:
		{
			const char* data = task.send_buffer.data() + task.write_offset;
			const std::size_t remaining = task.send_buffer.size() - task.write_offset;
			io_uring_prep_write(sqe_ptr, task.fd, data, remaining, 0);
			break;
		}
		case TaskType::kNone:
		{
			std::cerr << "unexpected kNone" << '\n';
			break;
		}
	}
	return true;
}

void Server::Submit()
{
	while (!defer_.empty())
	{
		auto index = defer_.front();
		defer_.pop_front();
		if (!Prepare(inprogress_[index]))
		{
			break;
		}
	}
	io_uring_submit(&ring_);
}

void Server::HandleTaskCompletion(io_uring_cqe& cqe)
{
	auto& task = *reinterpret_cast<Task*>(cqe.user_data);

	// finish task
	switch (task.type)
	{
		case TaskType::kAccept:
		{
			task.fd = cqe.res;
			if (task.fd < 0)
			{
				task.type = TaskType::kNone;
			}
			else
			{
				task.type = TaskType::kRead;
			}
			break;
		}
		case TaskType::kRead:
		{
			if (cqe.res > 0)
			{
				task.recv_buffer.append(task.read_buffer.data(), static_cast<std::size_t>(cqe.res));
				std::cout << "[RECEIVED " << cqe.res << " BYTES] "
							<< std::string_view(task.read_buffer.data(), static_cast<std::size_t>(cqe.res)) << '\n';

				while (!task.recv_buffer.empty())
				{
					Command command;
					auto parsed = command.Deserialize(task.recv_buffer);
					if (parsed.status == CommandParseStatus::kIncomplete)
					{
						break;
					}

					if (parsed.status == CommandParseStatus::kProtocolError)
					{
						task.pending_responses.push_back(Result(false,
							std::format("ERR protocol error: {}", parsed.error_message), "").Serialize());
						if (parsed.consumed_bytes > 0 && parsed.consumed_bytes <= task.recv_buffer.size())
						{
							task.recv_buffer.erase(0, parsed.consumed_bytes);
						}
						else
						{
							task.recv_buffer.clear();
						}
						continue;
					}

					command_designator_.Start(ExecuteCommandTask(task, std::move(command)));
					task.recv_buffer.erase(0, parsed.consumed_bytes);
				}

				// Keep the scheduler progression explicit for future awaitable I/O coroutines.
				command_designator_.ResumeAll();

				if (!task.pending_responses.empty())
				{
					task.send_buffer = std::move(task.pending_responses.front());
					task.pending_responses.pop_front();
					task.write_offset = 0;
					task.type = TaskType::kWrite;
				}
				else
				{
					task.type = TaskType::kRead;
				}
			}
			else if (cqe.res == 0)
			{
				// connection closed by peer
				task.type = TaskType::kNone; // close fd
			}
			else
			{
				// -EAGAIN, -EWOULDBLOCK, -ECONNRESET
				task.type = TaskType::kRead; // try again
			}
			break;
		}
		case TaskType::kWrite:
		{
			if (cqe.res > 0)
			{
				task.write_offset += static_cast<std::size_t>(cqe.res);
				if (task.write_offset >= task.send_buffer.size())
				{
					std::cout << "[SEND " << task.send_buffer.size() << " BYTES] " << task.send_buffer;
					if (!task.pending_responses.empty())
					{
						task.send_buffer = std::move(task.pending_responses.front());
						task.pending_responses.pop_front();
						task.write_offset = 0;
						task.type = TaskType::kWrite;
					}
					else
					{
						task.type = TaskType::kRead;
					}
				}
				else
				{
					task.type = TaskType::kWrite;
				}
			}
			else
			{
				// -ECONNRESET
				task.type = TaskType::kNone;
			}
			break;
		}
		case TaskType::kNone:
		{
			std::cerr << "unexpected kNone" << '\n';
		}
	}

	// prepare task
	switch (task.type)
	{
		case TaskType::kNone:
		{
			task.reset();
			task.type = TaskType::kAccept;
			// fall through
		}
		case TaskType::kAccept:
		{
			break;
		}
		case TaskType::kRead:
		{
			task.write_offset = 0;
			break;
		}
		case TaskType::kWrite:
		{
			break;
		}
	}
}

Result Server::Execute(const Command& command)
{
	auto it = handlers_.find(command.GetName());
	if (it == handlers_.end())
	{
		return Result(false, "ERR unknown command", "");
	}
	return it->second(command);
}

void Server::RegisterHandlers()
{
	handlers_["GET"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "ERR usage: GET key", "");
		}
		auto value = lru_cache_.Get(command.GetArguments()[0]);
		if (!value.has_value())
		{
			return Result(false, "ERR key not found", "");
		}
		return Result(true, "OK", *value);
	};

	handlers_["SET"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 2)
		{
			return Result(false, "ERR usage: SET key value", "");
		}
		lru_cache_.Set(command.GetArguments()[0], command.GetArguments()[1]);
		return Result(true, "OK", "");
	};

	handlers_["DELETE"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "ERR usage: DELETE key", "");
		}
		auto status = lru_cache_.Set(command.GetArguments()[0], std::nullopt);
		switch (status)
		{
			case KV::LRUCacheStatus::kOk:
				return Result(true, "OK", "");
			case KV::LRUCacheStatus::kInvalidArgument:
				return Result(false, "ERR key not found", "");
			case KV::LRUCacheStatus::kNonexistent:
				return Result(false, "ERR key not found", "");
			default:
				return Result(false, "ERR unknown error", "");
		};
	};

	handlers_["EXISTS"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "ERR usage: EXISTS key", "");
		}
		return Result(true, "OK", lru_cache_.Exists(command.GetArguments()[0]) ? "YES" : "NO");
	};
}

void Server::Run()
{
	if (!bound_)
	{
		throw std::runtime_error("server hasn't bound to a port");
	}
	if (listen(server_fd_, SOMAXCONN) < 0)
	{
		throw std::runtime_error(std::format("failed to listen on localhost:{}, errorno: {}", port_, errno));
	}
	io_uring_params params{};
	io_uring_queue_init_params(1024, &ring_, &params);

	/* prepare to accept `backlog_` requests */
	for (std::size_t i = 0; i < inprogress_.size(); i++)
	{
		/* get an SQE (Submission Queue Entry) */
		auto& task = inprogress_[i];
		task.type = TaskType::kAccept;
		Prepare(task);
	}

	/* submit accepts */
	Submit();

	while (true)
	{
		std::array<io_uring_cqe*, 32> cqes; // the kernel may generate multiple CQEs for one SQE
		std::size_t nready = io_uring_peek_batch_cqe(&ring_, cqes.data(), cqes.size());
		if (nready == 0)
		{
			auto err = io_uring_wait_cqe(&ring_, &cqes[0]);
			if (err < 0)
			{
				// error handling, e.g., -EAGAIN
				continue;
			}
			nready = 1;
		}
		for (std::size_t i = 0; i < nready; i++)
		{
			auto& task = *reinterpret_cast<Task*>(cqes[i]->user_data);
			HandleTaskCompletion(*cqes[i]);
			Prepare(task);
		}
		io_uring_cq_advance(&ring_, nready);
		Submit();
	}
}

} // namespace KV
