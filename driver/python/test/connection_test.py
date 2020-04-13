"""Tests for connection."""

import os
import test_data
import unittest

from sfdb import api_pb2
from connection import Connection, _parse_conn_string
from test.pylib import TestSFDB


class ConnectionTest(unittest.TestCase):
    _db = None

    @classmethod
    def setUpClass(cls):
        cls._db = TestSFDB()
        # spin up additional two instances for proper SFDB cluster init
        TestSFDB()
        TestSFDB()
        for i in TestSFDB.instances:
            i.start()

    @classmethod
    def tearDownClass(cls):
        TestSFDB.shutdown_all()

    def test_parse_connection_string_full(self):
        """Test full connection string."""
        conn_string = "admin:pass@localhost:27910/testdb?ttl=5&debug_string=1"
        addr, dbname, params, creds = _parse_conn_string(conn_string)
        self.assertTupleEqual(("localhost", "27910"), addr)
        self.assertEqual("testdb", dbname)
        self.assertDictEqual({"admin": "pass"}, creds)
        self.assertDictEqual({"ttl": "5", "debug_string": "1"}, params)

    def test_parse_connection_string_with_creds(self):
        """Test connection string with credentials, server, port, database."""
        conn_string = "admin@localhost:27910/testdb"
        addr, dbname, params, creds = _parse_conn_string(conn_string)
        self.assertTupleEqual(("localhost", "27910"), addr)
        self.assertEqual("testdb", dbname)
        self.assertDictEqual({"admin": None}, creds)
        self.assertIsNone(params)

    def test_parse_connection_string_short(self):
        """Test connection string with servername and database only."""
        conn_string = "localhost:27910/testdb"
        addr, dbname, params, creds = _parse_conn_string(conn_string)
        self.assertTupleEqual(("localhost", "27910"), addr)
        self.assertEqual("testdb", dbname)
        self.assertIsNone(creds)
        self.assertIsNone(params)

    def test_connection_grpc(self):
        """Test expects new instance of SFDB started (no tables)."""
        conn = Connection(("localhost", "27910"),
                          dbname="test", params="", creds="")
        req = api_pb2.ExecSqlRequest()
        for query, rows_expected in test_data.QUERIES.items():
            req.sql = query
            resp = conn.cmd_query(req)
            rows_num = len(resp.rows) if resp.rows else 0
            self.assertEqual(rows_num, rows_expected)


if __name__ == '__main__':
    unittest.main()
