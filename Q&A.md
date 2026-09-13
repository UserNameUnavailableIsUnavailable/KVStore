# Q&A

## Is it safe for a channel to hold raw coroutine handles?

Coroutine are attached to a channel on construction, detached on destruction.

## Why does `condition_variable` need the user to provide a lock?

```cpp
// thread A
if (!ready) wait();
```

```cpp
// thread B
ready = true;
notify();
```

There is a gap between predicate checking and wait in thread A:

```plain
A finds ready == false;
B sets ready = true;
B notifies;
A waits;
```

Is this case, A will never wake up. This is called **lost wakeup**.

The core of this issue is that predicate checking and wait are not performed atomically. This is exactly why we need another lock for `condition_variable`. `condition_variable` internally maintains a waiter list and a lock to ensure atomic push and pop, but that's another problem.

## The design of `ConditionVariable`
