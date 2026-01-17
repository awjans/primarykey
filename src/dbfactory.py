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
        self.port = port if port is not None else int(os.getenv("DB_PORT", 5432))
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

    TEST_INSERT_STATEMENT_PREDICATE: str = "INSERT INTO {table_name} (data) VALUES "
    TEST_SELECT_STATEMENT_PREDICATE: str = "SELECT * FROM {table_name} WHERE id in "
    TEST_UPDATE_STATEMENT_PREDICATE: str = "UPDATE {table_name} SET data = %s WHERE id in "
    TEST_DELETE_STATEMENT_PREDICATE: str = "DELETE FROM {table_name} WHERE id in "

    TEST_CREATE_TABLE: str = """
        DROP TABLE IF EXISTS {table_name};
        CREATE TABLE {table_name} (
            id {table_pk} PRIMARY KEY,
            data CHAR({char_length}) NOT NULL
        );
    """

    CHAR_BIGINT_LENGTH: int = 244
    CHAR_BIGINT_INSERT: str = ("A" * CHAR_BIGINT_LENGTH)
    CHAR_BIGINT_UPDATE: str = ("B" * CHAR_BIGINT_LENGTH)
    CHAR_UUID_LENGTH: int = 236
    CHAR_UUID_INSERT: str = ("A" * CHAR_UUID_LENGTH)
    CHAR_UUID_UPDATE: str = ("B" * CHAR_UUID_LENGTH)

    def get_create_table_statement(self, table_type: DBPrimaryKeyType) -> str:
        table_name: str = self.get_table_name(table_type)
        char_length: int = self.get_char_length(table_type)
        table_pk: str = self.get_table_pk(table_type)
        return self.TEST_CREATE_TABLE.format(table_name=table_name, table_pk=table_pk, char_length=char_length)

    def get_insert_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        predicate: str = self.TEST_INSERT_STATEMENT_PREDICATE.format(table_name=table_name)
        placeholders: str = ", ".join(["(%s)"] * batch_size)
        return predicate + placeholders

    def get_select_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        predicate: str = self.TEST_SELECT_STATEMENT_PREDICATE.format(table_name=table_name)
        placeholders: str = ", ".join(["%s"] * batch_size)
        return predicate + "(" + placeholders + ")"

    def get_update_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        predicate: str = self.TEST_UPDATE_STATEMENT_PREDICATE.format(table_name=table_name)
        placeholders: str = ", ".join(["%s"] * batch_size)
        return predicate + "(" + placeholders + ")"

    def get_delete_statement(self, table_type: DBPrimaryKeyType, batch_size: int = 1) -> str:
        table_name: str = self.get_table_name(table_type)
        predicate: str = self.TEST_DELETE_STATEMENT_PREDICATE.format(table_name=table_name)
        placeholders: str = ", ".join(["%s"] * batch_size)
        return predicate + "(" + placeholders + ")"

    def get_char_length(self, table_type: DBPrimaryKeyType) -> int:
        if table_type == DBPrimaryKeyType.BIGINT:
            return self.CHAR_BIGINT_LENGTH
        elif table_type == DBPrimaryKeyType.UUIDV4 or table_type == DBPrimaryKeyType.UUIDV7:
            return self.CHAR_UUID_LENGTH
        else:
            raise ValueError(f"Invalid table type '{table_type}'.")
    
    def get_char_data(self, table_type: DBPrimaryKeyType, operation: DBOperation) -> str:
        if table_type == DBPrimaryKeyType.BIGINT:
            if operation == DBOperation.INSERT:
                return self.CHAR_BIGINT_INSERT
            elif operation == DBOperation.UPDATE:
                return self.CHAR_BIGINT_UPDATE
            else:
                raise ValueError(f"Invalid operation '{operation}' for table type 'bigint'.")
        elif table_type == DBPrimaryKeyType.UUIDV4 or table_type == DBPrimaryKeyType.UUIDV7:
            if operation == DBOperation.INSERT:
                return self.CHAR_UUID_INSERT
            elif operation == DBOperation.UPDATE:
                return self.CHAR_UUID_UPDATE
            else:
                raise ValueError(f"Invalid operation '{operation}' for table type '{table_type}'.")
        else:
            raise ValueError(f"Invalid table type '{table_type}'.")


    def get_table_name(self, table_type: DBPrimaryKeyType) -> str:
        return f"test_{table_type.value}"
    

    def get_table_pk(self, table_type: DBPrimaryKeyType) -> str:
        if table_type == DBPrimaryKeyType.BIGINT:
            return "BIGSERIAL"
        elif table_type == DBPrimaryKeyType.UUIDV4:
            return "UUID DEFAULT gen_random_uuid()"
        elif table_type == DBPrimaryKeyType.UUIDV7:
            return "UUID DEFAULT uuidv7()"
        else:
            raise ValueError(f"Invalid table type '{table_type}'.")