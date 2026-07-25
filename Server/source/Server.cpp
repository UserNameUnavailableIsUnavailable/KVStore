#include "Server/Server.hpp"

#include <cerrno>
#include <format>
#include <iostream>
#include <stdexcept>

#include <liburing/io_uring.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include "Common/LRUCache.hpp"
#include "Common/Task.hpp"

namespace KV
{

Server::Server() :
	lru_cache_(32)
{
    server_fd_ = ::socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd_ < 0)
    {
        throw std::runtime_error(std::format("failed to create socket, errno: {}", errno));
    }

    int yes = 1;
    if (::setsockopt(server_fd_, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) < 0)
    {
        throw std::runtime_error(std::format("failed to set SO_REUSEADDR, errno: {}", errno));
    }

	RegisterHandlers();
}

void Server::Bind(std::uint16_t port)
{
	if (port_ != 0)
	{
		throw std::runtime_error(std::format("server already bound to port {}", port_));
	}
    ::sockaddr_in addr {
        .sin_family = AF_INET,
        .sin_port = htons(port),
        .sin_addr = {
            .s_addr = htonl(INADDR_ANY),
        },
        .sin_zero = {}
    };
    int ret = ::bind(server_fd_, reinterpret_cast<sockaddr*>(&addr), sizeof(addr));
    if (ret < 0)
    {
        throw std::runtime_error((std::format("failed to bind to port {}, errno: {}", port, errno)));
    }
    port_ = port;
}

Server::~Server() noexcept
{
	if (ring_initialized_)
	{
		::io_uring_queue_exit(&ring_);
	}
	if (server_fd_ >= 0)
	{
		::close(server_fd_);
	}
}

bool Server::PrepareTask(IOTask& task)
{
    if (task.GetType() == IOTaskType::kNone)
    {
        return false;
    }
    if (task.IsMultishot())
    {
        throw std::runtime_error("multishot tasks are not supported in this implementation");
    }

    auto* sqe = ::io_uring_get_sqe(&ring_);
    if (sqe == nullptr)
    {
		defer_.push_back(static_cast<std::size_t>(&task - io_tasks_.data()));
        return false;
    }

	::io_uring_sqe_set_data(sqe, &task);
    switch (task.GetType())
    {
        case IOTaskType::kAccept:
            task.WithMutableAcceptContext([sqe, server_fd = server_fd_](sockaddr_storage* address, socklen_t* address_length) {
                ::io_uring_prep_accept(sqe, server_fd, reinterpret_cast<sockaddr*>(address), address_length,
                    SOCK_CLOEXEC | SOCK_NONBLOCK);
            });
            break;
        case IOTaskType::kRecv:
				task.WithReceiveContext([sqe](int fd, char* data, std::size_t size) {
				::io_uring_prep_recv(sqe, fd, data, size, 0);
            });
            break;
        case IOTaskType::kSend:
		{
			task.WithSendContext([sqe](int fd, const char* data, std::size_t size) {
				::io_uring_prep_send(sqe, fd, data, size, 0);
			});
            break;
		}
        case IOTaskType::kNone:
            return false;
    }
	return true;
}

void Server::SubmitTasks()
{
	while (!defer_.empty())
	{
		auto index = defer_.front();
		defer_.pop_front();
		if (!PrepareTask(io_tasks_[index]))
		{
			break;
		}
	}
	const int result = ::io_uring_submit(&ring_);
	if (result < 0)
	{
		throw std::runtime_error(std::format("io_uring submission failed: {}", -result));
	}
}

void Server::HandleTaskCompletion(io_uring_cqe& cqe)
{
	auto& task = *static_cast<IOTask*>(::io_uring_cqe_get_data(&cqe));

	// finish task
	switch (task.GetType())
	{
		case IOTaskType::kAccept:
		{
			if (cqe.res < 0)
			{
				task.SetType(IOTaskType::kAccept);
			}
			else
			{
				task.SetFileDescriptor(cqe.res);
				task.SetType(IOTaskType::kRecv);
			}
			break;
		}
		case IOTaskType::kRecv:
		{
			task.CompleteReceive(cqe.res);
			if (cqe.res > 0)
			{
				Command command;
				auto parsed = task.WithConstReadBuffer([&command](const auto& buffer) {
					return command.Deserialize(std::string_view(buffer.data(), buffer.size()));
				});
				if (parsed.status == CommandParseStatus::kIncomplete)
				{
					task.SetType(IOTaskType::kRecv);
					break;
				}

				if (parsed.status == CommandParseStatus::kProtocolError)
				{
					const auto response = Result(false,
						std::format("protocol error: {}", parsed.error_message), "").Serialize();
					task.WithMutableWriteBuffer([&response](auto& buffer) {
						buffer.assign(response.begin(), response.end());
					});
					task.WithMutableReadBuffer([](auto& buffer) { buffer.clear(); });
					task.SetType(IOTaskType::kSend);
				}
				else
				{
					const bool has_extra_data = task.WithMutableReadBuffer([consumed = parsed.consumed_bytes](auto& buffer) {
						buffer.erase(buffer.begin(), buffer.begin() + static_cast<std::ptrdiff_t>(consumed));
						return !buffer.empty();
					});
					if (has_extra_data)
					{
						const auto response = Result(false, "pipelined commands are not supported", "").Serialize();
						task.WithMutableWriteBuffer([&response](auto& buffer) {
							buffer.assign(response.begin(), response.end());
						});
						task.WithMutableReadBuffer([](auto& buffer) { buffer.clear(); });
						task.SetType(IOTaskType::kSend);
					}
					else
					{
						command_designator_.Start(ExecuteCommandTask(task, std::move(command)));
						command_designator_.ResumeAll();
						task.SetType(IOTaskType::kSend);
					}
				}
			}
			else if (cqe.res == 0)
			{
				// connection closed by peer
				task.SetType(IOTaskType::kNone);
			}
			else
			{
				task.SetType(cqe.res == -EAGAIN || cqe.res == -EINTR ? IOTaskType::kRecv : IOTaskType::kNone);
			}
			break;
		}
		case IOTaskType::kSend:
		{
			if (cqe.res > 0)
			{
				if (task.CompleteSend(cqe.res))
				{
					task.WithConstWriteBuffer([](const auto& buffer) {
						std::cout << "[SEND " << buffer.size() << " BYTES] "
							<< std::string_view(buffer.data(), buffer.size());
					});
					task.SetType(IOTaskType::kRecv);
				}
				else
				{
					task.SetType(IOTaskType::kSend);
				}
			}
			else
			{
				// -ECONNRESET
				task.SetType(IOTaskType::kNone);
			}
			break;
		}
		case IOTaskType::kNone:
		{
			std::cerr << "unexpected kNone" << '\n';
		}
	}

	if (task.GetType() == IOTaskType::kNone)
	{
		task.Reset();
		task.SetType(IOTaskType::kAccept);
	}
}

Result Server::Execute(const Command& command)
{
	auto it = handlers_.find(command.GetName());
	if (it == handlers_.end())
	{
		return Result(false, "unknown command", "");
	}
	return it->second(command);
}

void Server::RegisterHandlers()
{
	handlers_["GET"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "usage: GET key", "");
		}
		auto value = lru_cache_.Get(command.GetArguments()[0]);
		if (!value.has_value())
		{
			return Result(false, "key not found", "");
		}
		return Result(true, "", *value);
	};

	handlers_["SET"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 2)
		{
			return Result(false, "usage: SET key value", "");
		}
		lru_cache_.Set(command.GetArguments()[0], command.GetArguments()[1]);
		return Result(true, "", "");
	};

	handlers_["DELETE"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "usage: DELETE key", "");
		}
		auto status = lru_cache_.Set(command.GetArguments()[0], std::nullopt);
		switch (status)
		{
			case LRUCacheStatus::kOk:
				return Result(true, "", "");
			case LRUCacheStatus::kInvalidArgument:
				return Result(false, "key not found", "");
			case LRUCacheStatus::kNonexistent:
				return Result(false, "key not found", "");
			default:
				return Result(false, "unknown error", "");
		};
	};

	handlers_["EXISTS"] = [this](const Command& command) -> Result
	{
		if (command.GetArguments().size() != 1)
		{
			return Result(false, "usage: EXISTS key", "");
		}
		return Result(true, "", lru_cache_.Exists(command.GetArguments()[0]) ? "YES" : "NO");
	};
}

void Server::Run()
{
	if (port_ == 0)
	{
		throw std::runtime_error("server hasn't bound to a port");
	}

	if (listen(server_fd_, SOMAXCONN) < 0)
	{
		throw std::runtime_error(std::format("failed to listen on localhost:{}, errorno: {}", port_, errno));
	}
	
	InitializeSubmissionQueue();

	for (std::size_t i = 0; i < io_tasks_.size(); i++)
	{
		/* get an SQE (Submission Queue Entry) */
		auto& task = io_tasks_[i];
		task.SetType(IOTaskType::kAccept);
		PrepareTask(task);
	}

	/* submit accepts */
	SubmitTasks();

	while (true)
	{
		std::array<io_uring_cqe*, 32> cqes; // the kernel may generate multiple CQEs for one SQE
		std::size_t nready = ::io_uring_peek_batch_cqe(&ring_, cqes.data(), cqes.size());
		if (nready == 0)
		{
			auto err = ::io_uring_wait_cqe(&ring_, &cqes[0]);
			if (err < 0)
			{
				if (err == -EINTR)
				{
					continue;
				}
				throw std::runtime_error(std::format("io_uring completion wait failed: {}", -err));
			}
			nready = 1;
		}
		for (std::size_t i = 0; i < nready; i++)
		{
			HandleTaskCompletion(*cqes[i]);
			auto& task = *static_cast<IOTask*>(::io_uring_cqe_get_data(cqes[i]));
			PrepareTask(task);
		}
		::io_uring_cq_advance(&ring_, nready);
		SubmitTasks();
	}
}

void Server::InitializeSubmissionQueue()
{
	::io_uring_params params{};
	params.flags = IORING_SETUP_CQSIZE;
	params.cq_entries = completion_queue_capacity_;
	const int result = ::io_uring_queue_init_params(submission_queue_capacity_, &ring_, &params);
	if (result < 0)
	{
		throw std::runtime_error(std::format("io_uring initialization failed: {}", -result));
	}
	ring_initialized_ = true;
}

} // namespace KV
