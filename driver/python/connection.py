# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Connection for the Google SFDB DB-API."""

import grpc
import logging
import sys
import platform
import exceptions
from cursor import Cursor
from logging import getLogger

from server import grpc_service_pb2_grpc
from server import grpc_service_pb2

TIMEOUT = 5
PYTHON_VERSION = ".".join([str(v) for v in sys.version_info[:3]])
PLATFORM = platform.platform()
_LOGGER = getLogger(__name__)


class Connection(object):
    """DB-API connection to SFDB"""

    def __init__(self, host_port, dbname, params, creds):
        """Creates and maintains gRPC channel.

        Args:
            host_port(tuple(str, str)): hostname:port 
            dbname(str): database name to connect (not implemented in SFDB)
            params(*str): connection parameters
            creds(tuple(str, str)): username:password

        Returns:
            Connection object

        Raises:
            InterfaceError: if there's any gRPC channel related exception
        """
        self.addr, self.port = host_port  # (ipv4:port|ipv6:port|hostname:port)
        self._creds = creds
        self._dbname = dbname
        self._params = params
        self._timeout = TIMEOUT
        self._grpc_channel = None
        self._stub = None
        self._errors = {}

        _LOGGER.debug(u"SFDB connector. Python Version: %s, Platform: %s",
                      PYTHON_VERSION, PLATFORM)
        self._connect()

    def _connect(self):
        try:
            self._grpc_channel = grpc.insecure_channel(
                "{}:{}".format(self.addr, self.port))
            self._stub = grpc_service_pb2_grpc.SfdbServiceStub(
                self._grpc_channel)
            _LOGGER.debug("gRPC channel and stub initialized")
        except grpc.RpcError as rpc_error:
            err_msg = "{}: {}".format(
                rpc_error.code().name, rpc_error.details())
            _LOGGER.error("gRPC channel error %s", err_msg)
            raise exceptions.InterfaceError(err_msg)

    def close(self):
        """Close the connection now.

        Connection will not be usable and will raise exception."""
        self._stub = None
        self._grpc_channel.close()
        _LOGGER.info("gRPC connection closed.")

    def commit(self):
        """SFDB does not support transactions."""
        pass

    def rollback(self):
        """SFDB does not support transactions."""
        pass

    def cursor(self, cursor_class=Cursor):
        """Creates cursor object using the connection.

        Each statement should create a new cursor object."""
        _LOGGER.debug("Creating cursor.")
        if not self._grpc_channel:
            raise exceptions.InterfaceError()
        return cursor_class(self)

    def cmd_query(self, req):
        """Executes SQL protobuf over gRPC.

        Args:
            req: request protobuf

        Returns:
            responce protubuf

        Raises: 
            InterfaceError if there are any errors.
        """
        try:
            resp = self._stub.ExecSql(req)
        except grpc.RpcError as rpc_error:
            resp = str(rpc_error)
            self._errors = {"status_code": rpc_error.code().name,
                            "details": rpc_error.details()}
            _LOGGER.error("Error during request: %s", self._errors)
            raise exceptions.InterfaceError(self._errors)
        return resp

    def _redirect(self, new_sock):
        try:
            self.addr, self.port, _ = new_sock.split(":")
        except ValueError as e:
            raise exceptions.InterfaceError("Invalid redirect address:", e)
        self._connect()

    def __repr__(self):
        return str(list(map(str, (self.addr,
                                  self.port,
                                  self._dbname,
                                  self._params))))


def connect(conn_str):
    """Construct a DB-API connection to SFDB.

    Args:
        conn_str(str): string containing connection attributes

    Returns:
        Connection object

    Raises:
        None
    """
    addr, dbname, params, creds = _parse_conn_string(conn_str)
    return Connection(addr, dbname=dbname, params=params, creds=creds)


def _parse_conn_string(conn_str):
    """Parses connection string to Connection.

    Example of connection string:
    [username[:password]@]address/dbname[?param1=value1&...&paramN=valueN]
    returns: creds dict, address tuple, dbname, params dict

    Args:
        conn_str(str): string containing connection attributes

    Returns:
        tuple containing parsed connection attributes
    """
    conn_str = conn_str.strip()
    if not conn_str:
        raise exceptions.InterfaceError("Empty connection string.")
    if r'@' in conn_str:
        creds_str, conn_str = conn_str.split(r'@')
        if r':' in creds_str:
            username, password = creds_str.split(r':')
        else:
            username, password = creds_str, None
        creds = {username: password}
    else:
        creds = None
    if r'?' in conn_str:
        conn_str, params_str = conn_str.split(r'?')
        params = {}
        for param in params_str.split(r'&'):
            k, v = param.split(r'=')
            params[k] = v
    else:
        params = None
    conn_str, dbname = conn_str.split(r'/')
    sock = tuple(conn_str.split(r':'))
    return sock, dbname, params, creds
