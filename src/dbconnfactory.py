import os
import psycopg2

from psycopg2 import sql


class DBConnFactory:
    def __init__(self,
                 host: str | None = None,
                 port: int | None = None,
                 user: str | None = None,
                 password: str | None = None,
                 name: str | None = None) -> None:
        """
        Initialize a database connection factory.
        
        :param self: The instance of the class.
        :type self: DBConnFactory
        :param host: The Hostname of the database server.
        :type host: str | None
        :param port: The Port number of the database server.
        :type port: int | None
        :param user: The Username for database authentication.
        :type user: str | None
        :param password: The Password for database authentication.
        :type password: str | None
        :param name: The Name of the database.
        :type name: str | None
        """
        self.host = host if host is not None else os.getenv("DB_HOST", "localhost")
        self.port = port if port is not None else int(os.getenv("DB_PORT", 5432))
        self.user = user if user is not None else os.getenv("DB_USER", "postgres")
        self.password = password if password is not None else os.getenv("DB_PASSWORD", "password")
        self.name = name if name is not None else os.getenv("DB_NAME", None)
    
    def get_connection(self) -> psycopg2.extensions.connection:
        """
        Create and return a new database connection.
        Creates the database if it does not already exist.

        :return: A new database connection.
        :rtype: psycopg2.extensions.connection
        """
        try:
            # Check if the database exists; if not, create it
            if self.name is not None:
                # Connect to the default database to check/create the target database
                with psycopg2.connect(
                    host=self.host,
                    port=self.port,
                    user=self.user,
                    password=self.password,
                ) as conn:
                    # Check if the database exists
                    with conn.cursor() as cursor:
                        sql_query: str = 'SELECT 1 FROM pg_database WHERE datname = %s;'
                        cursor.execute(sql_query, (self.name,))
                        exists = cursor.fetchall()
                        cursor.execute("COMMIT;")
                        if not exists:
                            # Create the database
                            sqlstmt: sql.Composable = sql.SQL('CREATE DATABASE {}').format(sql.Identifier(self.name,))
                            cursor.execute(sqlstmt)
            # Now connect to the target database
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