#include <iostream>
#include <type_traits>

template <typename C>
class Channel {
   public:
    auto submit() { return static_cast<C*>(this)->submit(); }
};

class DerivedChannel : public Channel<DerivedChannel> {
   public:
    struct Payload {
        int value;
    };

    Payload& submit() { return payload_; }

   private:
    Payload payload_;
};

int main(int argc, char* argv[]) {
    DerivedChannel dc;
    Channel<DerivedChannel>& c = dc;
    std::cout << c.submit().value;
    return 0;
}
