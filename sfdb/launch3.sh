#!/bin/bash
##
## Copyright (c) 2019 Google LLC.
##
## Licensed to the Apache Software Foundation (ASF) under one
## or more contributor license agreements.  See the NOTICE file
## distributed with this work for additional information
## regarding copyright ownership.  The ASF licenses this file
## to you under the Apache License, Version 2.0 (the
## "License"); you may not use this file except in compliance
## with the License.  You may obtain a copy of the License at
##
##   http://www.apache.org/licenses/LICENSE-2.0
##
## Unless required by applicable law or agreed to in writing,
## software distributed under the License is distributed on an
## "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
## KIND, either express or implied.  See the License for the
## specific language governing permissions and limitations
## under the License.
##
##

# Launches 3 instances locally, in ports 27910, 27911, and 27912.
# Ctrl-C kills everything.

set -e

bazel build //sfdb:sfdb

# enable logging
export GLOG_logtostderr=1

BINARY="../bazel-bin/sfdb/sfdb"
CMD="$BINARY"
declare -a PORTS=(27910 27911 27912)

for port in "${PORTS[@]}"; do
  TARGETS="$TARGETS,localhost:$port"
done
TARGETS=${TARGETS:1}

for port in "${PORTS[@]}"; do
  $CMD --port=$port --raft_my_target=localhost:$port --raft_targets=$TARGETS &
done

# Wait until Ctrl-C and kill everything.
trap 'killall' INT
killall() {
  trap '' INT TERM
  echo "Killing everything"
  kill -TERM 0
  wait
}
cat
