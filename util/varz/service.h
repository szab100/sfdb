/*
 * Copyright (c) 2019 Google LLC.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
#ifndef UTIL_VARZ_SERVICE_H_
#define UTIL_VARZ_SERVICE_H_

#include <stdint.h>
#include <string>
#include <thread>
#include <vector>

#include "absl/flags/flag.h"
#include "absl/strings/string_view.h"
#include "absl/synchronization/mutex.h"
#include "util/macros/macros.h"
#include "util/varz/httplib.h"

ABSL_DECLARE_FLAG(bool, varz_enable);
ABSL_DECLARE_FLAG(int32_t, varz_port);
ABSL_DECLARE_FLAG(std::string, varz_host);

namespace varz {
class VarZ;

namespace internal {
class VarZService {
public:
  bool Start();
  void Stop();

  void PublishVar(const VarZ &var);

  static VarZService* Instance();
private:
  VarZService() = default;
  ~VarZService();

  std::thread thread_;
  mutable absl::Mutex mutex_;
  httplib::Server server_;
  std::vector<const VarZ*> published_varz_;

  DISALLOW_COPY_AND_ASSIGN(VarZService);
};

}  // namespace internal
}  // namespace varz

#endif  // UTIL_VARZ_SERVICE_H_