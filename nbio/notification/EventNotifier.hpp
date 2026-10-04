#pragma once

#include <cstdint>
#include <system_error>

#include <nbio/utility/Expected.hpp>

namespace nbio::notification {
class EventNotifier {
   public:
	EventNotifier();
	~EventNotifier() noexcept;

	EventNotifier(const EventNotifier&) = delete;
	EventNotifier& operator=(const EventNotifier&) = delete;
	EventNotifier(EventNotifier&&) = delete;
	EventNotifier& operator=(EventNotifier&&) = delete;

	std::uintptr_t native_handle() const noexcept { return handle_; }
	void NonBlocking(bool enabled = true);
	void Notify();
    utility::expected<void, std::error_code> Wait();

   private:
	std::uintptr_t handle_;
};
}  // namespace nbio::notification
