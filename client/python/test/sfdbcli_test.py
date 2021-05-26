import unittest

from driver.python.connection import connect
from test.pylib import TestSFDB
from driver.python.test.test_data import QUERIES


class CLITest(unittest.TestCase):
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

    def test_query(self):
        # test assumes 5 rows exist in database
        conn = connect("localhost:27910/test")
        cur = conn.cursor()
        for q, r in QUERIES.items():
            cur.execute(q)
            self.assertEqual(r, cur.rowcount)


if __name__ == '__main__':
    unittest.main()
