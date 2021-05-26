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

"""Cursor for SFDB DB-API.

Used to execute single query. No transaction support.
"""

import collections
import json
import exceptions
import logging

from server import grpc_service_pb2_grpc
from server import grpc_service_pb2_grpc
from sfdb import api_pb2

from google.protobuf import descriptor as _descriptor
from google.protobuf import reflection as _reflection
from google.protobuf import message

_LOGGER = logging.getLogger(__name__)
RETRIES = 3


class Cursor(object):
    """DB-API Cursor to SFDB.

    Args:
        connection (driver.python.Connection):
            A DB-API connection to SFDB.
    """

    def __init__(self, connection):
        self._connection = connection
        self.description = None
        # Per PEP 249: The attribute is -1 in case no .execute*() has been
        # performed on the cursor or the rowcount of the last operation
        # cannot be determined by the interface.
        self.rowcount = -1
        # Per PEP 249: The arraysize attribute defaults to 1, meaning to fetch
        # a single row at a time. However, we deviate from that, and set the
        # default to None, allowing the backend to automatically determine the
        # most appropriate size.
        self.arraysize = None
        self._query_data = None
        self._errors = None
        self._handler = {
            api_pb2.ExecSqlResponse.OK: self._handle_OK,
            api_pb2.ExecSqlResponse.ERROR: self._handle_ERROR,
            api_pb2.ExecSqlResponse.REDIRECT: self._handle_REDIRECT,
        }

    @property
    def connection(self):
        return self._connection

    def _set_rowcount(self, query_results):
        """Set the rowcount from query results.

        Normally, this sets rowcount to the number of rows returned by the
        query, but if it was a DML statement, it sets rowcount to the number
        of modified rows.

        Args:
            query_results (Dictionary):
                Results of a query.
        """
        total_rows = 0
        num_dml_affected_rows = query_results.num_dml_affected_rows

        if query_results.total_rows is not None and query_results.total_rows > 0:
            total_rows = query_results.total_rows
        if num_dml_affected_rows is not None and num_dml_affected_rows > 0:
            total_rows = num_dml_affected_rows
        self.rowcount = total_rows

    def _handle(self, request):
        for _ in range(RETRIES):
            try:
                response = self._connection.cmd_query(request)
            except exceptions.InterfaceError:
                break
            result = self._handler[response.status](response)
            if result:
                return True
        return False

    def _handle_OK(self, response):
        _LOGGER.debug("Handling OK: %s", response)
        self.rowcount = 0
        if response.rows:
            descriptors = self._get_descriptors(response)
            self.rowcount = self._set_data(descriptors, response.rows)
        return True

    def _handle_REDIRECT(self, response):
        _LOGGER.debug("Redirecting to: %s", response.redirect)
        if response.redirect:
            self._connection._redirect(response.redirect)
        return False

    def _handle_ERROR(self, response):
        _LOGGER.error("Server error")
        self._errors = {"FAILED": "In operation."}
        return False

    def execute(self, operation, parameters=None):
        """Prepare and execute a database operation.

        Args:
            operation (str): SFDB query string.

            parameters (Union[Mapping[str, Any], Sequence[Any]]):
                (Optional) dictionary or sequence of parameter values.
        """
        _LOGGER.debug('Operation: %s', operation)
        self._query_data = None
        request = api_pb2.ExecSqlRequest()
        request.sql = operation
        return self._handle(request)

    def executemany(self, operation, seq_of_parameters):
        """Prepare and execute a database operation multiple times.

        Args:
            operation (str): SFDB query string.

            seq_of_parameters (Union[Sequence[Mapping[str, Any], \
                Sequence[Any]]]):
                Sequence of many sets of parameter values.
        """
        for parameters in seq_of_parameters:
            self.execute(operation, parameters)

    def _get_descriptors(self, resp):
        """Get descriptors from gRPC response.

        Iterates over descriptors' protobufs in response. Converts descriptor
        protobuf to descriptor and save in dict.
        """
        msg_descs = {}
        prefix = r'/'  # row.type_url format is "/<descriptor_pb.name"
        for file_desc in resp.descriptors.file:
            for desc_pb in file_desc.message_type:
                desc_pb.name = "{}.{}".format(file_desc.package, desc_pb.name)
                desc_key = f'{prefix}{desc_pb.name}'
                msg_descs[desc_key] = _descriptor.MakeDescriptor(desc_pb)
        return msg_descs

    def _set_data(self, descriptors, rows):
        """Populates cursor data field.

        Args:
            descriptors (google.protobuf.descriptor.Descriptor):
                dictionary with descriptors.

            rows (google.protobuf.any_pb2.Any): list of raw row data

        Returns:
            int: number of rows
        """
        data = []
        for row in rows:
            descriptor = descriptors[row.type_url]
            row_dict = self._get_row_dict(descriptor, row.value)
            data.append(row_dict)
        self._query_data = data
        return len(data)

    def _get_row_dict(self, descriptor, row):
        """Retrieves row dictionary for given descriptor.

        Args:
            descriptor (google.protobuf.desriptor.Descriptor): descriptor.

            row (google.protobuf.any_pb2.Any): raw row.

        Returns:
            Iterable[Mapping]: A sequence of rows, represented as dictionaries.
        """
        rows = {}
        row_data = _reflection.ParseMessage(descriptor, row)
        for descriptor in row_data.DESCRIPTOR.fields:
            value = getattr(row_data, descriptor.name)
            rows[descriptor.name] = value
        return rows

    @property
    def json(self):
        if self._query_data:
            return json.dumps(self._query_data)

    # The following methods not implemented, although expected by DB-API 2.0
    def callproc(self, procname):
        raise NotImplementedError

    def close(self, operation, params=None):
        raise NotImplementedError

    def fetchone(self):
        raise NotImplementedError

    def fetchmany(self, size):
        raise NotImplementedError

    def nextset(self):
        raise NotImplementedError

    def setinputsizes(self, sizes):
        raise NotImplementedError

    def setoutputsize(self, size, column):
        raise NotImplementedError

    def _set_description(self, schema):
        """Set description from schema."""
        raise NotImplementedError
