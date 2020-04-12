# SFDB - Simple Fast Database

> Experimental distributed in-memory database, entirely based on protocol buffers, gRPC and the Raft consensus algorithm.

[![Build Status](https://travis-ci.org/szab100/sfdb.svg?branch=master)](https://travis-ci.org/szab100/sfdb)

## Getting Started
>You can simply open, build and run this project by clicking the button below.<br>Everything is preinstalled, so simply ignore the <em>Prerequisites</em> section below.
>
>[![Open in Gitpod](https://gitpod.io/button/open-in-gitpod.svg)](https://gitpod.io/#https://github.com/szab100/sfdb)

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

### Vagrant VM

Vagrant VM can be used to run bazel builds and tests in locally provisioned VM.

- Install [vagrant](https://www.vagrantup.com/downloads.html)
- Install [VirtualBox](https://www.virtualbox.org/wiki/Downloads)

Repo code is mounted in `/src` folder inside VM.

Commonly used commands:

```
vagrant up
vagrant ssh
  # cd /src
  # bazel build //sfdb:sfdb
vagrant halt
vagrant destroy
```
