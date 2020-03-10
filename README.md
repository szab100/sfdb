SFDB - Simple Fast Database

Experimental distributed in-memory database based on protobuf & proto streams,
gRPC & Raft consensus algorithm.

## Getting Started

### Prerequisites
- Bazel v0.29.1: download the binary matching your platform from here: https://github.com/bazelbuild/bazel/releases/tag/0.29.1

### Build
```
bazel build //...
```

### Run Tests
```
bazel test //...
```

### Start SFDB nodes

The provided launch3.sh script starts 3 sfdb nodes on localhost (ports 27910, 27911,
27912).

```
./sfdb/launch3.sh
```
