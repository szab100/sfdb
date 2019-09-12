#include "util/net/port.h"

#if defined(__linux__)
#include <unistd.h>
#include <netinet/ip.h>
#include <sys/socket.h>
#else
  #error("This code is intended to be run on POSIX Linux")
#endif

#include "glog/logging.h"

#if defined(__linux__) && defined(_POSIX_VERSION)
int PickUpFreeLocalPort() {
  // Code is intended to work on POSIX Linux
  int s;

  s = socket(AF_INET, SOCK_STREAM, 0);
  CHECK(s != -1);

  struct sockaddr_in sin;
  bzero((char *) &sin, sizeof(sin));
  sin.sin_addr.s_addr = INADDR_ANY;
  sin.sin_family = AF_INET;
  sin.sin_port = 0;

  CHECK(bind(s, (struct sockaddr *)&sin, sizeof(sin)) != -1);

  socklen_t len = sizeof(sin);
  CHECK(getsockname(s, (struct sockaddr *)&sin, &len) != -1);

  close(s);

  return ntohs(sin.sin_port);
}
#endif