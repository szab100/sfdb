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
#ifndef UTIL_VARZ_VARZ_H_
#define UTIL_VARZ_VARZ_H_

#include <stdint.h>

#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <variant>


#include "absl/strings/string_view.h"
#include "absl/strings/str_format.h"
#include "absl/synchronization/mutex.h"
#include "absl/types/variant.h"

namespace varz {
namespace internal {
class VarZService;
}

class VarZ {
 public:
  enum class VarType { INT, DBL, STR };

  VarZ(absl::string_view name, int64_t initial_value);
  VarZ(absl::string_view name, double initial_value);
  VarZ(absl::string_view name, const std::string &initial_value);
  ~VarZ();

  const std::string &Name() const {
    return name_;
  }

  VarType Type() const {
    return type_;
  }

  void SetValue(int64_t i64);
  void SetValue(double dbl);
  void SetValue(const std::string &str);

  int64_t AsInt() const;
  double AsDbl() const;
  const std::string &AsString() const;

  std::string ToString() const;
private:
  // TODO: use vector/list to allow multiple cbs?
  std::function<void(const VarZ&)> notify_change_cb_;
  VarType type_;
  std::string name_;
  absl::variant<int64_t, double, std::string> value_;
  mutable absl::Mutex mutex_;
};

template <typename T, typename T2 = void>
struct VarZTypeTraits {};

template <typename T>
struct VarZTypeTraits<
    T, typename std::enable_if<std::is_integral<T>::value>::type> {
  static const VarZ::VarType var_type = VarZ::VarType::INT;
  using type = int64_t;
  using value_accessor_type = decltype(&VarZ::AsInt);

  static type GetValue(const VarZ &var) { return var.AsInt(); }
};

template <typename T>
struct VarZTypeTraits<
    T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
  static const VarZ::VarType var_type = VarZ::VarType::DBL;
  using type = double;
};

template <typename T>
struct VarZTypeTraits<
    T, typename std::enable_if<std::is_same<T, std::string>::value>::type> {
  static const VarZ::VarType var_type = VarZ::VarType::STR;
  using type = std::string;
};

template <typename T, typename T2 = void>
struct VarZValueAccessor {};

template <typename T>
struct VarZValueAccessor<
    T, typename std::enable_if<std::is_integral<T>::value>::type> {
  using value_type = typename VarZTypeTraits<T>::type;
  static value_type GetValue(const VarZ &var) { return var.AsInt(); }
};

template <typename T>
struct VarZValueAccessor<
    T, typename std::enable_if<std::is_floating_point<T>::value>::type> {
  using value_type = typename VarZTypeTraits<T>::type;
  static value_type GetValue(const VarZ &var) { return var.AsDbl(); }
};

template <typename T>
struct VarZValueAccessor<
    T, typename std::enable_if<std::is_same<T, std::string>::value>::type> {
  using value_type = typename VarZTypeTraits<T>::type;
  static value_type GetValue(const VarZ &var) { return var.AsString(); }
};

#ifdef DISABLE_VARZ
template <typename T>
class TypedVarZ {
 public:
  TypedVarZ(absl::string_view name, T &&initial_value) {}
  TypedVarZ &operator=(const T &t) { return *this; }

  operator T() const { return T{}; }
};

inline bool StartVarZService() { return false; }
inline void StopVarZService() {}
#else
template <typename T>
class TypedVarZ : protected VarZ {
 public:
  using var_type_t = typename VarZTypeTraits<T>::type;
  TypedVarZ(absl::string_view name, var_type_t &&initial_value) : VarZ(name, var_type_t(initial_value)) {}
  TypedVarZ &operator=(const var_type_t &t) { SetValue(t); return *this; }

  operator var_type_t() const { return VarZValueAccessor<T>::GetValue(); }
};

bool StartVarZService();
void StopVarZService();

#endif

}  // namespace varz

#define DECLARE_VARZ(type_name, name) varz::TypedVarZ<type_name> VARZ_##name;
#define DEFINE_VARZ(type_name, name, initial_value) varz::TypedVarZ<type_name> VARZ_##name(#name, initial_value);

#endif  //  UTIL_VARZ_VARZ_H_