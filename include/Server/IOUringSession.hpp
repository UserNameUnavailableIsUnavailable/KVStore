#pragma once

#include "Common/Session.hpp"

namespace KV
{
enum class IOUringSessionState
{
    kIdle,
    kAccepting,
    kReceiving,
    kSending,
};

class IOUringSession final : public Session
{
public:
    IOUringSession() : Session(NetworkingModel::kProactor) {}

    IOUringSessionState GetState() const
    {
        return state_;
    }

    void SetState(IOUringSessionState state)
    {
        state_ = state;
    }

    void Reset()
    {
        Session::Reset();
        state_ = IOUringSessionState::kIdle;
        is_multishot_ = false;
    }

private:
    IOUringSessionState state_ = IOUringSessionState::kIdle;
    bool is_multishot_ = false;
};
} // namespace KV