import logging
import os
import sys
import unittest
from dotenv import load_dotenv

sys.path.append(os.path.abspath('./src'))

from dbfactory import DBFactory

class TestDBFactory(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        load_dotenv()
        self.factory: DBFactory | None = DBFactory()

    def tearDown(self):
        self.factory = None

    def test_connection(self):
        self.assertIsNotNone(self.factory)
        with self.factory.get_connection() as conn: # type: ignore
            self.assertIsNotNone(conn)

if __name__ == '__main__':
    unittest.main()