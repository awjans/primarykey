import argparse
import asyncio
import logging
import os
import pandas as pd
import sys

from dotenv import load_dotenv

from dbfactory import DBFactory, DBOperation, DBPrimaryKeyType
from testpk import TestPrimaryKey

LOGFILE_TMPLT: str = "{pktype}_{operation}_{workers}_{batchsize}_{operations}.log"
METRICFILE_TMPLT: str = "{pktype}_{operation}_{workers}_{batchsize}_{operations}.csv"

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
    arg.add_argument("--loglevel", help="Logging level (Defaults to INFO)",
                     choices=[e for e in logging._nameToLevel.keys()], default="INFO", type=str)
    arg.add_argument("--logdir", help="Log directory (Defaults to .)", type=str, default=".")
    arg.add_argument("--pktype", help="Primary key type to test",
                     choices=[e.value for e in DBPrimaryKeyType], type=str, default=DBPrimaryKeyType.BIGINT.value)
    arg.add_argument("--operation", help="Operation to perform",
                     choices=[e.value for e in DBOperation], type=str, default=DBOperation.INSERT.value)
    arg.add_argument("--workers", help="Number of worker threads (Defaults to 1)",
                     choices=[1, 2, 4, 8, 16], type=int, default=1)
    arg.add_argument("--batchsize", help="Batch size for operations (Defaults to 1)",
                     choices=[1, 2, 10, 25, 50, 100, 250, 500, 1000], type=int, default=1)
    arg.add_argument("--operations", help="Number of operations to perform (Defaults to 1000)",
                     type=int, default=1000)
    arg.add_argument("--metricsdir", help=f"Metrics output directory (defaults to $PWD)", type=str, default=".")
    
    args = arg.parse_args(sys.argv[1:])

    os.makedirs(args.logdir, exist_ok=True)
    log_path = get_log_path(args.logdir, args.pktype, args.operation, args.workers, args.batchsize, args.operations)
    logging.basicConfig(level=getattr(logging, args.loglevel.upper(), logging.INFO),
                        filename=log_path, filemode='w',
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    if args.operations < (args.batchsize * args.workers):
        raise ValueError("The number of operations must be greater than or equal to the batch size.")
    if (args.operations % (args.batchsize * args.workers)) != 0:
        raise ValueError("The number of operations must be a multiple of the batch size.")

    os.makedirs(args.metricsdir, exist_ok=True)
    metrics_path = get_metrics_path(args.metricsdir, args.pktype, args.operation, args.workers, args.batchsize, args.operations)
    logging.info(f"Metrics will be written to: {metrics_path}")

    load_dotenv()

    db_factory: DBFactory = DBFactory(
        host=args.host,
        port=args.port,
        user=args.user,
        password=args.password,
        dbname=args.dbname
    )
    tester: TestPrimaryKey = TestPrimaryKey(
        dbfactory=db_factory,
        pktype=DBPrimaryKeyType(args.pktype),
        operation=DBOperation(args.operation),
        workers=args.workers,
        batchsize=args.batchsize,
        operations=args.operations
    )
    results: pd.DataFrame = asyncio.run(tester.run_test())
    results.to_csv(metrics_path, index=False)

def get_metrics_path(metrics_dir: str, pktype: str, operation: str, workers: int, batchsize: int, operations: int):
    metrics_file: str = METRICFILE_TMPLT.format(
        pktype=pktype,
        operation=operation,
        workers=workers,
        batchsize=batchsize,
        operations=operations
    )
    metrics_path: str = os.path.join(metrics_dir, metrics_file)
    return metrics_path

def get_log_path(log_dir: str, pktype: str, operation: str,  workers: int, batchsize: int, operations: int) -> str:
    log_file: str = LOGFILE_TMPLT.format(
        pktype=pktype,
        operation=operation,
        workers=workers,
        batchsize=batchsize,
        operations=operations
    )
    log_path: str = os.path.join(log_dir, log_file)
    return log_path


if __name__ == "__main__":
    main()
