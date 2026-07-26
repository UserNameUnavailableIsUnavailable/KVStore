#include "Server/IOUringServer.hpp"

#include <iostream>

namespace KV
{
IOUringServer::~IOUringServer() noexcept
{
	if (ring_initialized_)
	{
		::io_uring_queue_exit(&ring_);
	}
}

bool IOUringServer::PrepareTask(IOUringSession& session)
{
	auto& connection = session.GetConnection();
	if (session.GetState() == IOUringSessionState::kIdle)
    {
        return false;
    }

    auto* sqe = ::io_uring_get_sqe(&ring_);
    if (sqe == nullptr)
    {
		defer_.push_back(static_cast<std::size_t>(&session - sessions_.data()));
        return false;
    }

	::io_uring_sqe_set_data(sqe, &session);
	switch (session.GetState())
    {
		case IOUringSessionState::kAccepting:
			connection.WithMutableAcceptContext([sqe, server_fd = GetSocketHandle()](sockaddr_storage* address, socklen_t* address_length) {
                ::io_uring_prep_accept(sqe, server_fd, reinterpret_cast<sockaddr*>(address), address_length,
                    SOCK_CLOEXEC | SOCK_NONBLOCK);
            });
            break;
		case IOUringSessionState::kReceiving:
				session.WithReceiveContext([sqe](int fd, char* data, std::size_t size) {
				::io_uring_prep_recv(sqe, fd, data, size, 0);
            });
            break;
		case IOUringSessionState::kSending:
		{
			session.WithSendContext([sqe](int fd, const char* data, std::size_t size) {
				::io_uring_prep_send(sqe, fd, data, size, 0);
			});
            break;
		}
		case IOUringSessionState::kIdle:
            return false;
    }
	return true;
}

void IOUringServer::SubmitTasks()
{
	while (!defer_.empty())
	{
		auto index = defer_.front();
		defer_.pop_front();
		if (!PrepareTask(sessions_[index]))
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

void IOUringServer::HandleTaskCompletion(io_uring_cqe& cqe)
{
	auto& session = *static_cast<IOUringSession*>(::io_uring_cqe_get_data(&cqe));
	auto& connection = session.GetConnection();

	// finish task
	switch (session.GetState())
	{
		case IOUringSessionState::kAccepting:
		{
			if (cqe.res < 0)
			{
				session.SetState(IOUringSessionState::kAccepting);
			}
			else
			{
				connection.SetFileDescriptor(cqe.res);
				session.SetState(IOUringSessionState::kReceiving);
			}
			break;
		}
		case IOUringSessionState::kReceiving:
		{
			session.CompleteReceive(cqe.res);
			if (cqe.res > 0)
			{
				Command command;
				auto parsed = session.WithReadBuffer([&command](const auto& buffer) {
					return command.Deserialize(std::string_view(buffer.data(), buffer.size()));
				});
				if (parsed.status == CommandParseStatus::kIncomplete)
				{
					session.SetState(IOUringSessionState::kReceiving);
					break;
				}

				if (parsed.status == CommandParseStatus::kProtocolError)
				{
					const auto response = Result(false,
						std::format("protocol error: {}", parsed.error_message), "").Serialize();
					session.WithWriteBuffer([&response](auto& buffer) {
						buffer.assign(response.begin(), response.end());
					});
					session.ClearReadBuffer();
					session.SetState(IOUringSessionState::kSending);
				}
				else
				{
					const bool has_extra_data = session.ConsumeReadBuffer(parsed.consumed_bytes);
					if (has_extra_data)
					{
						const auto response = Result(false, "pipelined commands are not supported", "").Serialize();
						session.WithWriteBuffer([&response](auto& buffer) {
							buffer.assign(response.begin(), response.end());
						});
						session.ClearReadBuffer();
						session.SetState(IOUringSessionState::kSending);
					}
					else
					{
						const auto response = Execute(command).Serialize();
						session.WithWriteBuffer([&response](auto& buffer) {
							buffer.assign(response.begin(), response.end());
						});
						session.SetState(IOUringSessionState::kSending);
					}
				}
			}
			else if (cqe.res == 0)
			{
				// connection closed by peer
				session.SetState(IOUringSessionState::kIdle);
			}
			else
			{
				session.SetState(cqe.res == -EAGAIN || cqe.res == -EINTR ? IOUringSessionState::kReceiving : IOUringSessionState::kIdle);
			}
			break;
		}
		case IOUringSessionState::kSending:
		{
			if (cqe.res > 0)
			{
				if (session.CompleteSend(cqe.res))
				{
					session.WithConstWriteBuffer([](const auto& buffer) {
						std::cout << "[SEND " << buffer.size() << " BYTES] "
							<< std::string_view(buffer.data(), buffer.size());
					});
					session.SetState(IOUringSessionState::kReceiving);
				}
				else
				{
					session.SetState(IOUringSessionState::kSending);
				}
			}
			else
			{
				// -ECONNRESET
				session.SetState(IOUringSessionState::kIdle);
			}
			break;
		}
		case IOUringSessionState::kIdle:
		{
			std::cerr << "unexpected kNone" << '\n';
		}
	}

	if (session.GetState() == IOUringSessionState::kIdle)
	{
		session.Reset();
		session.SetState(IOUringSessionState::kAccepting);
	}
}

void IOUringServer::Run()
{
	if (GetPort() == 0)
	{
		throw std::runtime_error("server hasn't bound to a port");
	}

	if (listen(GetSocketHandle(), SOMAXCONN) < 0)
	{
		throw std::runtime_error(std::format("failed to listen on localhost:{}, errorno: {}", GetPort(), errno));
	}
	
	InitializeSubmissionQueue();

	for (std::size_t i = 0; i < sessions_.size(); i++)
	{
		/* get an SQE (Submission Queue Entry) */
		auto& session = sessions_[i];
		session.SetState(IOUringSessionState::kAccepting);
		PrepareTask(session);
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
			auto& session = *static_cast<IOUringSession*>(::io_uring_cqe_get_data(cqes[i]));
			PrepareTask(session);
		}
		::io_uring_cq_advance(&ring_, nready);
		SubmitTasks();
	}
}

void IOUringServer::InitializeSubmissionQueue()
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
}