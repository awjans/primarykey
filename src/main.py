import argparse
import logging
import os
import sys
from dotenv import load_dotenv

from dbfactory import DBFactory, DBPrimaryKeyType


def main() -> None:
    """
    Run PrimaryKey Test using the specified configuration.
    :return: None
    :rtype: None
    """
    arg = argparse.ArgumentParser(description="Run PrimaryKey Test using the specified configuration.")
    arg.add_argument("--host", help="Database host (Defaults to $DB_HOST or localhost)", type=str)
    arg.add_argument("--port", help="Database port (Defaults to $DB_PORT or 5432)", type=int)
    arg.add_argument("--user", help="Database user (Defaults to $DB_USER or postgres)", type=str)
    arg.add_argument("--password", help="Database password (Defaults to $DB_PASSWORD or password)", type=str)
    arg.add_argument("--dbname", help="Database name (Defaults to $DB_NAME or testdb)", type=str)
    arg.add_argument("--loglevel", help="Logging level (Defaults to INFO)", choices=[e for e in logging._nameToLevel.keys()], default="INFO", type=str)
    arg.add_argument("--logfile", help="Log file path (Defaults to ./out.log)", type=str)
    arg.add_argument("--pktype", help="Primary key type to test", choices=[e.value for e in DBPrimaryKeyType], required=True, type=str)
    arg.add_argument("--metricsfile", help="Metrics output file (defaults to ./{pktype}.log)", type=str)
    
    args = arg.parse_args(sys.argv[1:])
    load_dotenv()

    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO),
                        filename=args.logfile,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    db_factory: DBFactory = DBFactory(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname
    )
    with db_factory.get_connection() as db_conn:
        logging.info("Database connection established successfully. {}".format(db_conn.dsn))
        with db_conn.cursor() as cursor:
            cursor.execute("SELECT version();")
            db_version = cursor.fetchall()
            if db_version is None or len(db_version) == 0:
                raise Exception("Failed to retrieve database version.")
            db_version = db_version[0][0]
            logging.info(f"Database version: {db_version}")


if __name__ == "__main__":
    main()
