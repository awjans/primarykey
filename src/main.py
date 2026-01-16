from dotenv import load_dotenv

from dbconnfactory import DBConnFactory

CHAR_BIGINT = ("A" * 244)
CHAR_UUID = ("A" * 236)

def main() -> None:
    """
    Docstring for main
    """
    db_conn_factory: DBConnFactory = DBConnFactory()
    with db_conn_factory.get_connection() as db_conn:
        print("Database connection established successfully. {}".format(db_conn.dsn))
        with db_conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchall()
            if db_version is None or len(db_version) == 0:
                raise Exception("Failed to retrieve database version.")
            else:
                db_version = db_version[0][0]
            print(f"Database version: {db_version}")
            print("Creating test tables...")
            cursor.execute("DROP TABLE IF EXISTS test_bigint; CREATE TABLE test_bigint (id BIGSERIAL PRIMARY KEY, data CHAR(244) NOT NULL);")
            cursor.execute("DROP TABLE IF EXISTS test_uuidv4; CREATE TABLE test_uuidv4 (id UUID PRIMARY KEY DEFAULT gen_random_uuid(), data CHAR(236) NOT NULL);")
            cursor.execute("DROP TABLE IF EXISTS test_uuidv7; CREATE TABLE test_uuidv7 (id UUID PRIMARY KEY DEFAULT uuidv7(), data CHAR(236) NOT NULL);")
            print("Created test tables successfully.")


if __name__ == "__main__":
    load_dotenv()
    main()
