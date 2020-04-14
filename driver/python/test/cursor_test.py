"""Tests for cursor."""

import unittest
import test_data

from connection import Connection, _parse_conn_string
from cursor import Cursor
from test.pylib import TestSFDB


class CursorTest(unittest.TestCase):
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

    def setUp(self):
        addr, dbname, params, creds = _parse_conn_string(
            "localhost:27910/test")
        self.conn = Connection(addr, dbname, params, creds)
        self.cur = Cursor(self.conn)

    def test_select(self):
        for query, result in test_data.QUERIES.items():
            self.cur.execute(query)
            self.assertEqual(result, self.cur.rowcount)

    def test_no_table(self):
        self.cur.execute("SELECT id FROM Test;")
        self.assertDictEqual({'FAILED': 'In operation.'},
                             self.cur._errors)

    def test_json(self):
        for query, _ in test_data.QUERIES.items():
            self.cur.execute(query)
        self.assertEqual('[{"_1": "bob"}, {"_1": "jon"}]', self.cur.json)


if __name__ == '__main__':
    unittest.main()
