import enum
import logging
import os
import psycopg2

from psycopg2 import sql


class DBOperation(enum.Enum):
    INSERT = "insert"
    SELECT = "select"
    UPDATE = "update"
    DELETE = "delete"


class DBPrimaryKeyType(enum.Enum):
    BIGINT = "bigint"
    UUIDV4 = "uuidv4"
    UUIDV7 = "uuidv7"


class DBFactory:
    def __init__(self,
                 host: str | None = None,
                 port: int | None = None,
                 user: str | None = None,
                 password: str | None = None,
                 dbname: str | None = None) -> None:
        """
        Initialize a database connection factory.
        
        :param self: The instance of the class.
        :type self: DBConnFactory
        :param host: The Hostname of the database server. Defaulst to $DB_HOST or "localhost".
        :type host: str | None
        :param port: The Port number of the database server. Defaults to $DB_PORT or 5432.
        :type port: int | None
        :param user: The Username for database authentication. Defaults to $DB_USER or "postgres".
        :type user: str | None
        :param password: The Password for database authentication. Defaults to $DB_PASSWORD or "password".
        :type password: str | None
        :param name: The Name of the database. Defaults to $DB_NAME or "testdb".
        :type name: str | None
        """
        self.host = host if host is not None else os.getenv("DB_HOST", "localhost")
        self.port = port if port is not None else int(os.getenv("DB_PORT", "5432"))
        self.user = user if user is not None else os.getenv("DB_USER", "postgres")
        self.password = password if password is not None else os.getenv("DB_PASSWORD", "password")
        self.name = dbname if dbname is not None else os.getenv("DB_NAME", "testdb")
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Create and return a new database connection.
        Creates the database if it does not already exist.

        :return: A new database connection.
        :rtype: psycopg2.extensions.connection
        """
        try:
            logging.info(f"Connecting to database '{self.name}'.")
            conn = psycopg2.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                password=self.password,
                dbname=self.name
            )
            return conn
        except psycopg2.Error as e:
            raise Exception(f"Failed to connect to database: {e}")

    TABLE_CREATE: str = """DROP TABLE IF EXISTS {table_name};
CREATE TABLE {table_name} (
    id {table_pk} PRIMARY KEY,
    data CHAR({char_length}) NOT NULL
);"""
    TABLE_CHECK: str = "SELECT to_regclass('public.{table_name}');"
    TABLE_DROP: str = "DROP TABLE IF EXISTS {table_name};"

    INSERT_STATEMENT: str = "INSERT INTO {table_name} (data) VALUES {placeholders} RETURNING id;"
    SELECT_STATEMENT: str = "SELECT * FROM {table_name} WHERE id in ({placeholders});"
    UPDATE_STATEMENT: str = "UPDATE {table_name} SET data = %s WHERE id in ({placeholders});"
    DELETE_STATEMENT: str = "DELETE FROM {table_name} WHERE id in ({placeholders});"

    CHAR_BIGINT_LENGTH: int = 244
    CHAR_BIGINT_INSERT: str = ("A" * CHAR_BIGINT_LENGTH)
    CHAR_BIGINT_UPDATE: str = ("B" * CHAR_BIGINT_LENGTH)
    CHAR_UUID_LENGTH: int = 236
    CHAR_UUID_INSERT: str = ("A" * CHAR_UUID_LENGTH)
    CHAR_UUID_UPDATE: str = ("B" * CHAR_UUID_LENGTH)

    def get_table_create_statement(self, table_type: DBPrimaryKeyType) -> str:
        table_name: str = self.get_table_name(table_type)
        char_length: int = self.get_char_length(table_type)
        table_pk: str = self.get_table_pk(table_type)
        stmt: str = self.TABLE_CREATE.format(table_name=table_name, table_pk=table_pk, char_length=char_length)
        logging.debug(f"Create table statement for type '{table_type}': {stmt}")
        return stmt

    def get_table_check_statement(self, table_type: DBPrimaryKeyType) -> str:
        table_name: str = self.get_table_name(table_type)
        stmt: str = self.TABLE_CHECK.format(table_name=table_name)
        logging.debug(f"Check table statement for type '{table_type}': {stmt}")
        return stmt
 
    def get_table_drop_statement(self, table_type: DBPrimaryKeyType) -> str:
        table_name: str = self.get_table_name(table_type)
        stmt: str = self.TABLE_DROP.format(table_name=table_name)
        logging.debug(f"Drop table statement for type '{table_type}': {stmt}")
        return stmt
    
    def get_table_operation_statement(self, table_type: DBPrimaryKeyType, operation: DBOperation, batch_size: int = 1) -> str:
        if operation == DBOperation.INSERT:
            return self._get_insert_statement(table_type, batch_size)
        elif operation == DBOperation.SELECT:
            return self._get_select_statement(table_type, batch_size)
        elif operation == DBOperation.UPDATE:
            return self._get_update_statement(table_type, batch_size)
        elif operation == DBOperation.DELETE:
            return self._get_delete_statement(table_type, batch_size)
        raise ValueError(f"Invalid operation '{operation}'.")

    def _get_insert_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        placeholders: str = ", ".join(["(%s)"] * batch_size)
        stmt: str = self.INSERT_STATEMENT.format(table_name=table_name, placeholders=placeholders)
        logging.debug(f"Insert statement for type '{table_type}' and batch size {batch_size}: {stmt}")
        return stmt

    def _get_select_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        placeholders: str = ", ".join(["%s"] * batch_size)
        stmt: str = self.SELECT_STATEMENT.format(table_name=table_name, placeholders=placeholders)
        logging.debug(f"Select statement for type '{table_type}' and batch size {batch_size}: {stmt}")
        return stmt

    def _get_update_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        placeholders: str = ", ".join(["%s"] * batch_size)
        stmt: str = self.UPDATE_STATEMENT.format(table_name=table_name, placeholders=placeholders)
        logging.debug(f"Update statement for type '{table_type}' and batch size {batch_size}: {stmt}")
        return stmt

    def _get_delete_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        placeholders: str = ", ".join(["%s"] * batch_size)
        stmt: str = self.DELETE_STATEMENT.format(table_name=table_name, placeholders=placeholders)
        logging.debug(f"Delete statement for type '{table_type}' and batch size {batch_size}: {stmt}")
        return stmt

    def get_char_length(self, table_type: DBPrimaryKeyType) -> int:
        if table_type == DBPrimaryKeyType.BIGINT:
            return self.CHAR_BIGINT_LENGTH
        elif table_type == DBPrimaryKeyType.UUIDV4 or table_type == DBPrimaryKeyType.UUIDV7:
            return self.CHAR_UUID_LENGTH
        raise ValueError(f"Invalid table type '{table_type}'.")
    
    def get_char_data(self, table_type: DBPrimaryKeyType, operation: DBOperation) -> str:
        if table_type == DBPrimaryKeyType.BIGINT:
            if operation == DBOperation.INSERT:
                return self.CHAR_BIGINT_INSERT
            elif operation == DBOperation.UPDATE:
                return self.CHAR_BIGINT_UPDATE
        elif table_type == DBPrimaryKeyType.UUIDV4 or table_type == DBPrimaryKeyType.UUIDV7:
            if operation == DBOperation.INSERT:
                return self.CHAR_UUID_INSERT
            elif operation == DBOperation.UPDATE:
                return self.CHAR_UUID_UPDATE
        raise ValueError(f"Invalid table type '{table_type}' or operation '{operation}'.")

    def get_table_name(self, table_type: DBPrimaryKeyType) -> str:
        if table_type in DBPrimaryKeyType:
            return f"test_{table_type.value.lower()}"
        raise ValueError(f"Invalid table type '{table_type}'.")

    def get_table_pk(self, table_type: DBPrimaryKeyType) -> str:
        if table_type == DBPrimaryKeyType.BIGINT:
            return "BIGSERIAL"
        elif table_type == DBPrimaryKeyType.UUIDV4:
            return "UUID DEFAULT gen_random_uuid()"
        elif table_type == DBPrimaryKeyType.UUIDV7:
            return "UUID DEFAULT uuidv7()"
        raise ValueError(f"Invalid table type '{table_type}'.")
 