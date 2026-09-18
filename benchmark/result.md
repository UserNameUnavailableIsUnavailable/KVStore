# Benchmark

Run under 1,000 clients, 1,000,000 requests.

## pipeline

### redis

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -P 16
SET: 1919385.75 requests per second, p50=6.759 msec
GET: 2212389.50 requests per second, p50=4.743 msec
```

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -P 64
SET: 3154574.00 requests per second, p50=17.807 msec
GET: 3676470.50 requests per second, p50=14.727 msec
```

### KVStore (epoll multiplexer)

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666 -P 16
SET: 294898.25 requests per second, p50=1.135 msec                      
GET: 339904.84 requests per second, p50=0.903 msec
```

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666 -P 64
SET: 194212.47 requests per second, p50=8.943 msec                      
GET: 213766.56 requests per second, p50=8.679 msec
```

### KVStore (io_uring multiplexer)

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666 -P 16
SET: 338409.47 requests per second, p50=0.967 msec                    
GET: 340020.41 requests per second, p50=0.951 msec            
```

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666 -P 64
SET: 498753.09 requests per second, p50=2.455 msec                      
GET: 534473.50 requests per second, p50=2.863 msec                    
```

## no pipeline

### redis

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q
SET: 166112.95 requests per second, p50=3.335 msec                    
GET: 150240.38 requests per second, p50=3.743 msec
```

### KVStore (epoll multiplexer)

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666
SET: 132996.41 requests per second, p50=6.879 msec                    
GET: 145095.77 requests per second, p50=6.135 msec
```

### KVStore (io_uring multiplexer)

```
redis-benchmark -t set,get,del -c 1000 -n 1000000 -q -h 127.0.0.1 -p 6666
SET: 134210.17 requests per second, p50=6.447 msec                    
GET: 142369.02 requests per second, p50=5.959 msec
```
