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

"""Python DB API v2.0 (DB-API):
https://www.python.org/dev/peps/pep-0249 """


from connection import connect
from connection import Connection
from cursor import Cursor
from exceptions import Warning
from exceptions import Error
from exceptions import InterfaceError
from exceptions import DatabaseError
from exceptions import DataError
from exceptions import OperationalError
from exceptions import IntegrityError
from exceptions import InternalError
from exceptions import ProgrammingError
from exceptions import NotSupportedError
from dbtypes import Binary
from dbtypes import Date
from dbtypes import DateFromTicks
from dbtypes import Timestamp
from dbtypes import TimestampFromTicks
from dbtypes import BINARY
from dbtypes import DATETIME
from dbtypes import NUMBER
from dbtypes import ROWID
from dbtypes import STRING

apilevel = '2.0'

paramstyle = 'pyformat'

__all__ = [
    'apilevel',
    'paramstyle',
    'connect',
    'Connection',
    'Cursor',
    'Warning',
    'Error',
    'InterfaceError',
    'DatabaseError',
    'DataError',
    'OperationalError',
    'IntegrityError',
    'InternalError',
    'ProgrammingError',
    'NotSupportedError',
    'Binary',
    'Date',
    'DateFromTicks',
    'Time',
    'TimeFromTicks',
    'Timestamp',
    'TimestampFromTicks',
    'BINARY',
    'DATETIME',
    'NUMBER',
    'ROWID',
    'STRING',
]
