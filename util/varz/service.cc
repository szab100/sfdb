#include "util/varz/service.h"

#include <string>

#include "absl/strings/str_cat.h"
#include "glog/logging.h"
#include "util/varz/varz.h"

ABSL_FLAG(bool, varz_enable, true, "VarZ enabled");
ABSL_FLAG(int32_t, varz_port, 8080, "VarZ HTTP port");
ABSL_FLAG(std::string, varz_host, "127.0.0.1", "VarZ HTTP server address");

namespace varz {
namespace internal {
using namespace httplib;
using absl::GetFlag;

bool VarZService::Start() {
  absl::MutexLock lock(&mutex_);

  std::string varz_content = "<html><body>";
  for (const auto& var : published_varz_) {
    const auto& var_name = var->Name();

    // Register handler for this var
    std::string endpoint = "/varz/" + var_name;
    server_.Get(endpoint.c_str(), [&var](const Request& req, Response& res) {
      res.set_content(absl::StrFormat("%s = %s", var->Name(), var->ToString()),
                      "text/plain");
    });

    varz_content += absl::StrCat("<a href=\"\\varz\\", var_name, "\">", var_name, "</a><br>");
  }
  varz_content += "</body></html>";

  // Register handler for listing all varz
  server_.Get("/varz", [varz_content](const Request& req, Response& res) {
    res.set_content(varz_content, "text/html");
  });

  auto host = GetFlag(FLAGS_varz_host);
  auto port = GetFlag(FLAGS_varz_port);

  thread_ = std::thread([this, host, port](){
    LOG(INFO) << "Starting varz service on " << absl::GetFlag(FLAGS_varz_host) << ":" << absl::GetFlag(FLAGS_varz_port);
    server_.listen(host.c_str(), port);
  });

  return true;
}

void VarZService::Stop() {
  absl::MutexLock lock(&mutex_);

  if (server_.is_running()) {
    server_.stop();
  }

  if (thread_.joinable()) {
    thread_.join();
  }
}

// TODO: we need a way to unregister var when it is destroyed!
// NOTE: This code is run when static vars are initialized, do not
//       expect any service will be initialized at this moment.
void VarZService::PublishVar(const VarZ& var) {
  published_varz_.push_back(&var);
}

VarZService* VarZService::Instance() {
  if (!GetFlag(FLAGS_varz_enable)) {
    return nullptr;
  }

  static VarZService service;

  return &service;
}

VarZService::~VarZService() {
  // Stop has running check inside
  Stop();
}

}  // namespace internal
}  // namespace varz