#pragma once

#include <cstdint>
#include <system_error>

#include <NBIO/Utility/Expected.hpp>

namespace NBIO::Notification {
class EventNotifier {
   public:
	EventNotifier();
	~EventNotifier() noexcept;

	EventNotifier(const EventNotifier&) = delete;
	EventNotifier& operator=(const EventNotifier&) = delete;
	EventNotifier(EventNotifier&&) = delete;
	EventNotifier& operator=(EventNotifier&&) = delete;

	std::uintptr_t native_handle() const noexcept { return handle_; }
	void non_blocking(bool enabled = true);
	void notify();
    Utility::expected<void, std::error_code> wait();

   private:
	std::uintptr_t handle_;
};
}  // namespace NBIO::Notification
