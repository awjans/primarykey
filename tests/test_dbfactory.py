from ast import Delete
import logging
import os
import sys
import unittest
from dotenv import load_dotenv

sys.path.append(os.path.abspath('./src'))

from dbfactory import DBFactory, DBPrimaryKeyType, DBOperation

class TestDBFactory(unittest.TestCase):
    def setUp(self):
        logging.basicConfig(level=logging.DEBUG)
        load_dotenv()
        self.factory: DBFactory = DBFactory()

    def tearDown(self):
        pass

    def test_get_char_length(self):
        self.assertIsNotNone(self.factory)
        bigint_length: int = self.factory.get_char_length(DBPrimaryKeyType.BIGINT)
        self.assertEqual(bigint_length, 244)
        uuidv4_length: int = self.factory.get_char_length(DBPrimaryKeyType.UUIDV4)
        self.assertEqual(uuidv4_length, 236)
        uuidv7_length: int = self.factory.get_char_length(DBPrimaryKeyType.UUIDV7)
        self.assertEqual(uuidv7_length, 236)
        with self.assertRaises(ValueError):
            self.factory.get_char_length("INVALID_TYPE")  # type: ignore

    def test_get_char_data(self):
        self.assertIsNotNone(self.factory)
        bigint_insert = self.factory.get_char_data(DBPrimaryKeyType.BIGINT, DBOperation.INSERT)
        self.assertEqual(len(bigint_insert), 244)
        bigint_update = self.factory.get_char_data(DBPrimaryKeyType.BIGINT, DBOperation.UPDATE)
        self.assertEqual(len(bigint_update), 244)
        uuidv4_insert = self.factory.get_char_data(DBPrimaryKeyType.UUIDV4, DBOperation.INSERT)
        self.assertEqual(len(uuidv4_insert), 236)
        uuidv4_update = self.factory.get_char_data(DBPrimaryKeyType.UUIDV4, DBOperation.UPDATE)
        self.assertEqual(len(uuidv4_update), 236)
        uuidv7_insert = self.factory.get_char_data(DBPrimaryKeyType.UUIDV7, DBOperation.INSERT)
        self.assertEqual(len(uuidv7_insert), 236)
        uuidv7_update = self.factory.get_char_data(DBPrimaryKeyType.UUIDV7, DBOperation.UPDATE)
        self.assertEqual(len(uuidv7_update), 236)
        with self.assertRaises(ValueError):
            self.factory.get_char_data("INVALID_TYPE", DBOperation.INSERT)  # type: ignore
        with self.assertRaises(ValueError):
            self.factory.get_char_data(DBPrimaryKeyType.BIGINT, "INVALID_OPERATION")  # type: ignore

    def test_get_table_name(self):
        self.assertIsNotNone(self.factory)
        bigint_table = self.factory.get_table_name(DBPrimaryKeyType.BIGINT)
        self.assertEqual(bigint_table, "test_bigint")
        uuidv4_table = self.factory.get_table_name(DBPrimaryKeyType.UUIDV4)
        self.assertEqual(uuidv4_table, "test_uuidv4")
        uuidv7_table = self.factory.get_table_name(DBPrimaryKeyType.UUIDV7)
        self.assertEqual(uuidv7_table, "test_uuidv7")
        with self.assertRaises(ValueError):
            self.factory.get_table_name("INVALID_TYPE")  # type: ignore

    def test_get_table_pk(self):
        self.assertIsNotNone(self.factory)
        bigint_pk = self.factory.get_table_pk(DBPrimaryKeyType.BIGINT)
        self.assertEqual(bigint_pk, "BIGSERIAL")
        uuidv4_pk = self.factory.get_table_pk(DBPrimaryKeyType.UUIDV4)
        self.assertEqual(uuidv4_pk, "UUID DEFAULT gen_random_uuid()")
        uuidv7_pk = self.factory.get_table_pk(DBPrimaryKeyType.UUIDV7)
        self.assertEqual(uuidv7_pk, "UUID DEFAULT uuidv7()")
        with self.assertRaises(ValueError):
            self.factory.get_table_pk("INVALID_TYPE")  # type: ignore

    def test_get_table_create_statement(self):
        self.assertIsNotNone(self.factory)
        bigint_create_stmt: str = self.factory.get_table_create_statement(DBPrimaryKeyType.BIGINT)
        self.assertIsNotNone(bigint_create_stmt)
        self.assertEqual(bigint_create_stmt, """DROP TABLE IF EXISTS test_bigint;
CREATE TABLE test_bigint (
    id BIGSERIAL PRIMARY KEY,
    data CHAR(244) NOT NULL
);""")
        uuidv4_create_stmt: str = self.factory.get_table_create_statement(DBPrimaryKeyType.UUIDV4)
        self.assertIsNotNone(uuidv4_create_stmt)
        self.assertEqual(uuidv4_create_stmt, """DROP TABLE IF EXISTS test_uuidv4;
CREATE TABLE test_uuidv4 (
    id UUID DEFAULT gen_random_uuid() PRIMARY KEY,
    data CHAR(236) NOT NULL
);""")
        uuidv7_create_stmt: str = self.factory.get_table_create_statement(DBPrimaryKeyType.UUIDV7)
        self.assertIsNotNone(uuidv7_create_stmt)
        self.assertEqual(uuidv7_create_stmt, """DROP TABLE IF EXISTS test_uuidv7;
CREATE TABLE test_uuidv7 (
    id UUID DEFAULT uuidv7() PRIMARY KEY,
    data CHAR(236) NOT NULL
);""")

    def test_get_table_check_statement(self):
        self.assertIsNotNone(self.factory)
        uuidv4_check_stmt: str = self.factory.get_table_check_statement(DBPrimaryKeyType.UUIDV4)
        self.assertIsNotNone(uuidv4_check_stmt)
        self.assertEqual(uuidv4_check_stmt, "SELECT to_regclass('public.test_uuidv4');")
        bigint_check_stmt: str = self.factory.get_table_check_statement(DBPrimaryKeyType.BIGINT)
        self.assertIsNotNone(bigint_check_stmt)
        self.assertEqual(bigint_check_stmt, "SELECT to_regclass('public.test_bigint');")
        uuidv7_check_stmt: str = self.factory.get_table_check_statement(DBPrimaryKeyType.UUIDV7)
        self.assertIsNotNone(uuidv7_check_stmt)
        self.assertEqual(uuidv7_check_stmt, "SELECT to_regclass('public.test_uuidv7');")

    def test_get_table_drop_statement(self):
        self.assertIsNotNone(self.factory)
        uuidv4_drop_stmt: str = self.factory.get_table_drop_statement(DBPrimaryKeyType.UUIDV4)
        self.assertIsNotNone(uuidv4_drop_stmt)
        self.assertEqual(uuidv4_drop_stmt, "DROP TABLE IF EXISTS test_uuidv4;")
        bigint_drop_stmt: str = self.factory.get_table_drop_statement(DBPrimaryKeyType.BIGINT)
        self.assertIsNotNone(bigint_drop_stmt)
        self.assertEqual(bigint_drop_stmt, "DROP TABLE IF EXISTS test_bigint;")
        uuidv7_drop_stmt: str = self.factory.get_table_drop_statement(DBPrimaryKeyType.UUIDV7)
        self.assertIsNotNone(uuidv7_drop_stmt)
        self.assertEqual(uuidv7_drop_stmt, "DROP TABLE IF EXISTS test_uuidv7;")
    
    def test_get_table_operation_statement(self):
        self.assertIsNotNone(self.factory)
        insert_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.INSERT, batch_size=3)
        self.assertIsNotNone(insert_stmt)
        self.assertEqual(insert_stmt, "INSERT INTO test_uuidv4 (data) VALUES (%s), (%s), (%s) RETURNING id;")
        select_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.SELECT, batch_size=2)
        self.assertIsNotNone(select_stmt)
        self.assertEqual(select_stmt, "SELECT * FROM test_uuidv4 WHERE id in (%s, %s);")
        update_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.UPDATE, batch_size=4)
        self.assertIsNotNone(update_stmt)
        self.assertEqual(update_stmt, "UPDATE test_uuidv4 SET data = %s WHERE id in (%s, %s, %s, %s);")
        delete_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.DELETE, batch_size=1)
        self.assertIsNotNone(delete_stmt)
        self.assertEqual(delete_stmt, "DELETE FROM test_uuidv4 WHERE id in (%s);")
        with self.assertRaises(ValueError):
            self.factory.get_table_operation_statement("INVALID_TYPE", DBOperation.INSERT, batch_size=1)  # type: ignore
        with self.assertRaises(ValueError):
            self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, "INVALID_OPERATION", batch_size=1)  # type: ignore

    def test_table_bigint(self):
        self.assertIsNotNone(self.factory)
        with self.factory.get_connection() as conn:
            self.assertIsNotNone(conn)
            with conn.cursor() as cursor:
                self.assertIsNotNone(cursor)
                create_stmt = self.factory.get_table_create_statement(DBPrimaryKeyType.BIGINT)
                self.assertIsNotNone(create_stmt)
                cursor.execute(create_stmt)
                check_stmt = self.factory.get_table_check_statement(DBPrimaryKeyType.BIGINT)
                self.assertIsNotNone(check_stmt)
                cursor.execute(check_stmt)
                result = cursor.fetchone()
                if result is None:
                    self.fail("Table was not created properly.")
                self.assertIsNotNone(result)
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0], self.factory.get_table_name(DBPrimaryKeyType.BIGINT))
                insert_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.BIGINT, DBOperation.INSERT, batch_size=2)
                self.assertIsNotNone(insert_stmt)
                data_val: str = self.factory.get_char_data(DBPrimaryKeyType.BIGINT, DBOperation.INSERT)
                cursor.execute(insert_stmt, (data_val, data_val))
                inserted_ids = cursor.fetchall()
                self.assertIsNotNone(inserted_ids)
                self.assertEqual(len(inserted_ids), 2)
                select_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.BIGINT, DBOperation.SELECT, batch_size=2)
                self.assertIsNotNone(select_stmt)
                cursor.execute(select_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                selected_rows = cursor.fetchall()
                self.assertIsNotNone(selected_rows)
                self.assertEqual(len(selected_rows), 2)
                update_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.BIGINT, DBOperation.UPDATE, batch_size=2)
                self.assertIsNotNone(update_stmt)
                updated_data_val: str = self.factory.get_char_data(DBPrimaryKeyType.BIGINT, DBOperation.UPDATE)
                cursor.execute(update_stmt, (updated_data_val, inserted_ids[0][0], inserted_ids[1][0]))
                delete_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.BIGINT, DBOperation.DELETE, batch_size=2)
                self.assertIsNotNone(delete_stmt)
                cursor.execute(delete_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                drop_stmt = self.factory.get_table_drop_statement(DBPrimaryKeyType.BIGINT)
                self.assertIsNotNone(drop_stmt)
                cursor.execute(drop_stmt)

    def test_table_uuidv4(self):
        self.assertIsNotNone(self.factory)
        with self.factory.get_connection() as conn:
            self.assertIsNotNone(conn)
            with conn.cursor() as cursor:
                self.assertIsNotNone(cursor)
                create_stmt = self.factory.get_table_create_statement(DBPrimaryKeyType.UUIDV4)
                self.assertIsNotNone(create_stmt)
                cursor.execute(create_stmt)
                check_stmt = self.factory.get_table_check_statement(DBPrimaryKeyType.UUIDV4)
                self.assertIsNotNone(check_stmt)
                cursor.execute(check_stmt)
                result = cursor.fetchone()
                if result is None:
                    self.fail("Table was not created properly.")
                self.assertIsNotNone(result)
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0], self.factory.get_table_name(DBPrimaryKeyType.UUIDV4))
                insert_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.INSERT, batch_size=2)
                self.assertIsNotNone(insert_stmt)
                data_val: str = self.factory.get_char_data(DBPrimaryKeyType.UUIDV4, DBOperation.INSERT)
                cursor.execute(insert_stmt, (data_val, data_val))
                inserted_ids = cursor.fetchall()
                self.assertIsNotNone(inserted_ids)
                self.assertEqual(len(inserted_ids), 2)
                select_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.SELECT, batch_size=2)
                self.assertIsNotNone(select_stmt)
                cursor.execute(select_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                selected_rows = cursor.fetchall()
                self.assertIsNotNone(selected_rows)
                self.assertEqual(len(selected_rows), 2)
                update_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.UPDATE, batch_size=2)
                self.assertIsNotNone(update_stmt)
                updated_data_val: str = self.factory.get_char_data(DBPrimaryKeyType.UUIDV4, DBOperation.UPDATE)
                cursor.execute(update_stmt, (updated_data_val, inserted_ids[0][0], inserted_ids[1][0]))
                delete_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV4, DBOperation.DELETE, batch_size=2)
                self.assertIsNotNone(delete_stmt)
                cursor.execute(delete_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                drop_stmt = self.factory.get_table_drop_statement(DBPrimaryKeyType.UUIDV4)
                self.assertIsNotNone(drop_stmt)
                cursor.execute(drop_stmt)

    def test_table_uuidv7(self):
        self.assertIsNotNone(self.factory)
        with self.factory.get_connection() as conn:
            self.assertIsNotNone(conn)
            with conn.cursor() as cursor:
                self.assertIsNotNone(cursor)
                create_stmt = self.factory.get_table_create_statement(DBPrimaryKeyType.UUIDV7)
                self.assertIsNotNone(create_stmt)
                cursor.execute(create_stmt)
                check_stmt = self.factory.get_table_check_statement(DBPrimaryKeyType.UUIDV7)
                self.assertIsNotNone(check_stmt)
                cursor.execute(check_stmt)
                result = cursor.fetchone()
                if result is None:
                    self.fail("Table was not created properly.")
                self.assertIsNotNone(result)
                self.assertIsInstance(result, tuple)
                self.assertEqual(len(result), 1)
                self.assertEqual(result[0], self.factory.get_table_name(DBPrimaryKeyType.UUIDV7))
                insert_stmt: str = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV7, DBOperation.INSERT, batch_size=2)
                self.assertIsNotNone(insert_stmt)
                data_val: str = self.factory.get_char_data(DBPrimaryKeyType.UUIDV7, DBOperation.INSERT)
                cursor.execute(insert_stmt, (data_val, data_val))
                inserted_ids = cursor.fetchall()
                self.assertIsNotNone(inserted_ids)
                self.assertEqual(len(inserted_ids), 2)
                select_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV7, DBOperation.SELECT, batch_size=2)
                self.assertIsNotNone(select_stmt)
                cursor.execute(select_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                selected_rows = cursor.fetchall()
                self.assertIsNotNone(selected_rows)
                self.assertEqual(len(selected_rows), 2)
                update_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV7, DBOperation.UPDATE, batch_size=2)
                self.assertIsNotNone(update_stmt)
                updated_data_val: str = self.factory.get_char_data(DBPrimaryKeyType.UUIDV7, DBOperation.UPDATE)
                cursor.execute(update_stmt, (updated_data_val, inserted_ids[0][0], inserted_ids[1][0]))
                delete_stmt = self.factory.get_table_operation_statement(DBPrimaryKeyType.UUIDV7, DBOperation.DELETE, batch_size=2)
                self.assertIsNotNone(delete_stmt)
                cursor.execute(delete_stmt, (inserted_ids[0][0], inserted_ids[1][0]))
                drop_stmt = self.factory.get_table_drop_statement(DBPrimaryKeyType.UUIDV7)
                self.assertIsNotNone(drop_stmt)
                cursor.execute(drop_stmt)

if __name__ == '__main__':
    unittest.main()