#include "util/varz/varz.h"

#include "glog/logging.h"
#include "util/varz/service.h"

#ifndef DISABLE_VARZ

namespace varz {

bool StartVarZService() {
  auto service = internal::VarZService::Instance();
  if (service) {
    return service->Start();
  }

  return false;
}

void StopVarZService() {
  auto service = internal::VarZService::Instance();
  if (service) {
    service->Stop();
  }
}

VarZ::VarZ(absl::string_view name, int64_t initial_value)
    : type_(VarType::INT), name_(name), value_(int64_t(0)) {
  auto service = internal::VarZService::Instance();
  if (service) {
    service->PublishVar(*this);
  }
}

VarZ::VarZ(absl::string_view name, double initial_value)
    : type_(VarType::DBL), name_(name), value_(double(0.0)) {
  auto service = internal::VarZService::Instance();
  if (service) {
    service->PublishVar(*this);
  }
}

VarZ::VarZ(absl::string_view name, const std::string &initial_value)
    : type_(VarType::STR), name_(name), value_(std::string("")) {
  auto service = internal::VarZService::Instance();
  if (service) {
    service->PublishVar(*this);
  }
}

VarZ::~VarZ() {}

void VarZ::SetValue(int64_t i64) {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::INT);
  value_ = i64;
}

void VarZ::SetValue(double dbl) {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::DBL);
  value_ = dbl;
}

void VarZ::SetValue(const std::string &str) {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::STR);
  value_ = str;
}

int64_t VarZ::AsInt() const {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::INT);
  CHECK(absl::holds_alternative<int64_t>(value_));
  return absl::get<int64_t>(value_);
}

double VarZ::AsDbl() const {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::DBL);
  CHECK(absl::holds_alternative<double>(value_));
  return absl::get<double>(value_);
}

const std::string &VarZ::AsString() const {
  absl::MutexLock lock(&mutex_);
  CHECK(type_ == VarType::STR);
  CHECK(absl::holds_alternative<std::string>(value_));
  return absl::get<std::string>(value_);
}

std::string VarZ::ToString() const {
  switch (type_) {
    case VarType::INT:
      CHECK(absl::holds_alternative<int64_t>(value_));
      return absl::StrFormat("%d", absl::get<int64_t>(value_));
    case VarType::DBL:
      CHECK(absl::holds_alternative<double>(value_));
      return absl::StrFormat("%f", absl::get<double>(value_));
    case VarType::STR:
      CHECK(absl::holds_alternative<std::string>(value_));
      return absl::get<std::string>(value_);
    default:
      LOG(FATAL) << "Unknown varz var type";
  }
}

}  // namespace varz

#endif  // DISABLE_VARZ