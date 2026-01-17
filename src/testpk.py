import logging
import random
import time
import pandas as pd

from threading import Thread
from typing import Any

from dbfactory import DBFactory, DBOperation, DBPrimaryKeyType

class TestPrimaryKey:
    class TestPrimaryKeyWorker(Thread):
        def __init__(self,
                     id: int,
                     dbfactory: DBFactory,
                     pktype: DBPrimaryKeyType,
                     batchsize: int,
                     operations: int,
        ) -> None:
            super().__init__()
            self.id = id
            self.dbfactory = dbfactory
            self.pktype = pktype
            self.batchsize = batchsize
            self.operations = operations
            self.results = pd.DataFrame()

        def run(self) -> None:
            """
            Run the worker to perform primary key operations.
            """
            logging.info(f"Worker {self.id} starting test with PK type {self.pktype.value}")
            output: pd.DataFrame = pd.DataFrame()
            ins_done: int = 0
            ins_stmt: str = self.dbfactory.get_table_operation_statement(
                table_type=self.pktype,
                operation=DBOperation.INSERT,
                batch_size=self.batchsize
            )
            ins_args: list[str] = [self.dbfactory.get_char_data(self.pktype, DBOperation.INSERT)] * self.batchsize
            sel_done: int = 0
            sel_stmt: str = self.dbfactory.get_table_operation_statement(
                table_type=self.pktype,
                operation=DBOperation.SELECT,
                batch_size=self.batchsize
            )
            upd_done: int = 0
            upd_stmt: str = self.dbfactory.get_table_operation_statement(
                table_type=self.pktype,
                operation=DBOperation.UPDATE,
                batch_size=self.batchsize
            )
            upd_arg: str = self.dbfactory.get_char_data(self.pktype, DBOperation.UPDATE)
            del_done: int = 0
            del_stmt: str = self.dbfactory.get_table_operation_statement(
                table_type=self.pktype,
                operation=DBOperation.DELETE,
                batch_size=self.batchsize
            )
            keys: list[Any] = []
            with self.dbfactory.get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    while ins_done < self.operations:
                        logging.info(f"Worker {self.id} performing {DBOperation.INSERT.value} for batch size {self.batchsize}")
                        start = time.perf_counter()
                        cur.execute(ins_stmt, ins_args)
                        results: list[tuple] = cur.fetchall()
                        end = time.perf_counter()
                        logging.debug(f"Worker {self.id} inserted keys: {len(results)}")
                        keys.extend([row[0] for row in results])
                        output = pd.concat([output, pd.DataFrame({
                            "workerid": [self.id],
                            "operation": [DBOperation.INSERT.value],
                            "batchsize": [self.batchsize],
                            "duration": [end - start],
                        })], ignore_index=True)
                        ins_done += self.batchsize

                with conn.cursor() as cur:
                    while sel_done < self.operations:
                        logging.info(f"Worker {self.id} performing {DBOperation.SELECT.value} for batch size {self.batchsize}")
                        sel_keys = random.sample(keys, self.batchsize)
                        start = time.perf_counter()
                        cur.execute(sel_stmt, sel_keys)
                        results: list[tuple] = cur.fetchall()
                        end = time.perf_counter()
                        logging.debug(f"Worker {self.id} selected keys: {len(results)}")
                        output = pd.concat([output, pd.DataFrame({
                            "workerid": [self.id],
                            "operation": [DBOperation.SELECT.value],
                            "batchsize": [self.batchsize],
                            "duration": [end - start],
                        })], ignore_index=True)
                        sel_done += self.batchsize

                with conn.cursor() as cur:
                    while upd_done < self.operations:
                        logging.info(f"Worker {self.id} performing {DBOperation.UPDATE.value} for batch size {self.batchsize}")
                        upd_keys = random.sample(keys, self.batchsize)
                        start = time.perf_counter()
                        cur.execute(upd_stmt, [upd_arg] + upd_keys)
                        end = time.perf_counter()
                        output = pd.concat([output, pd.DataFrame({
                            "workerid": [self.id],
                            "operation": [DBOperation.UPDATE.value],
                            "batchsize": [self.batchsize],
                            "duration": [end - start],
                        })], ignore_index=True)
                        upd_done += self.batchsize

                with conn.cursor() as cur:
                    while del_done < self.operations:
                        logging.info(f"Worker {self.id} performing {DBOperation.DELETE.value} for batch size {self.batchsize}")
                        del_keys = keys[del_done:del_done+self.batchsize]
                        start = time.perf_counter()
                        cur.execute(del_stmt, del_keys)
                        end = time.perf_counter()
                        output = pd.concat([output, pd.DataFrame({
                            "workerid": [self.id],
                            "operation": [DBOperation.DELETE.value],
                            "batchsize": [self.batchsize],
                            "duration": [end - start],
                        })], ignore_index=True)
                        del_done += self.batchsize

            self.results = output
            logging.info(f"Worker {self.id} completed all operations.")

    def __init__(self,
                 dbfactory: DBFactory,
                 pktype: DBPrimaryKeyType,
                 workers: int,
                 batchsize: int,
                 operations: int
    ) -> None:
        self.dbfactory = dbfactory
        self.pktype = pktype
        self.workers = workers
        self.batchsize = batchsize
        self.operations = operations
    
    def run_test(self) -> pd.DataFrame:
        """
        Run the primary key test.
        :return: DataFrame with metrics for all workers
        :rtype: pd.DataFrame
        """
        ops_per_worker = self.operations // self.workers
        workers = [
            self.TestPrimaryKeyWorker(
                id=i,
                dbfactory=self.dbfactory,
                pktype=self.pktype,
                batchsize=self.batchsize,
                operations=ops_per_worker,
            ) for i in range(self.workers)
        ]
        
        self.dbfactory.create_table(self.pktype)

        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join()
        results: list[pd.DataFrame] = [worker.results for worker in workers]

        self.dbfactory.drop_table(self.pktype)

        return pd.concat(results, ignore_index=True)