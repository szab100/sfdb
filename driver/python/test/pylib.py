import subprocess
import os
import time
from http import HTTPStatus
from urllib.request import urlopen
from urllib.error import URLError

BAZEL_SFDB_BINARY = "sfdb/sfdb"
PORTS = (27910, 27911, 27912)
HOST = "127.0.0.1"
TIMEOUT = 3


class TestSFDB(object):
    instances = []

    @classmethod
    def pick_port(cls, port):
        ports = [i.port for i in cls.instances]
        return max(ports) + 1 if port in ports else port

    @classmethod
    def shutdown_all(cls):
        for i in cls.instances:
            i.shutdown()

    @staticmethod
    def raft_targets():
        return ",".join(["{}:{}".format(HOST, p) for p in PORTS])

    def __init__(self, name=HOST, port=PORTS[0], timeout=TIMEOUT):
        self._name = name
        self._port = self.pick_port(port)
        self._timeout = timeout
        self._process = None
        self._args = {
            "port": self._port,
            "raft_my_target": self.sock,
            "raft_targets": TestSFDB.raft_targets(),
            "raft_impl": "braft",
            "log_alsologtostderr": "true",
        }
        TestSFDB.instances.append(self)

    def start(self):
        self._process = subprocess.Popen(
            [BAZEL_SFDB_BINARY, *self.cmdline_args],
            stdout=None, stderr=None
        )
        return self.wait_for_start()

    def shutdown(self):
        if self._process:
            self._process.terminate()
            self._process.wait()
            self._process = None

    def wait_for_start(self):
        time.sleep(self._timeout)  # wait to complete db startup
        if self.is_healthy:
            return True
        self.shutdown()
        return False

    @property
    def is_healthy(self):
        try:
            with urlopen(self.health_check_url) as resp:
                if HTTPStatus.OK == resp.getcode():
                    return True
        except URLError:
            pass
        return False

    @property
    def port(self):
        return self._port

    @property
    def cmdline_args(self):
        return ["--{}={}".format(k, v) for k, v in self._args.items()]

    @property
    def sock(self):
        return "{}:{}".format(self._name, self._port)

    @property
    def health_check_url(self):
        return "http://{}/health".format(self.sock)

    def __repr__(self):
        return self.sock
