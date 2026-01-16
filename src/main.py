from dotenv import load_dotenv

from dbconnfactory import DBConnFactory

def main() -> None:
    """
    Docstring for main
    """
    db_conn_factory: DBConnFactory = DBConnFactory()
    with db_conn_factory.get_connection() as db_conn:
        print("Database connection established successfully. {}".format(db_conn.dsn))
        with db_conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchone()
            print(f"Database version: {db_version}")
            cursor.execute("COMMIT;")
            cursor.execute("DROP TABLE IF EXISTS test_bigint;")
            cursor.execute("CREATE TABLE test_bigint (id BIGSERIAL PRIMARY KEY, data CHAR(248));")


if __name__ == "__main__":
    load_dotenv()
    main()
