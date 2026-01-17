import asyncio
import logging
import pandas as pd

from asyncio import Task, tasks

from dbfactory import DBFactory, DBOperation, DBPrimaryKeyType

class TestPrimaryKey:
    class TestPrimaryKeyWorker:
        def __init__(self,
                     id: int,
                     dbfactory: DBFactory,
                     pktype: DBPrimaryKeyType,
                     operation: DBOperation,
                     batchsize: int,
                     operations: int,
        ) -> None:
            self.id = id
            self.dbfactory = dbfactory
            self.pktype = pktype
            self.operation = operation
            self.batchsize = batchsize
            self.operations = operations

        async def run(self) -> pd.DataFrame:
            """
            Run the worker to perform primary key operations.
            :return: DataFrame with metrics for this worker
            :rtype: pd.DataFrame
            """
            output: pd.DataFrame = pd.DataFrame()
            ops_done: int = 0
            ins_stmt: str = self.dbfactory.get_table_operation_statement(
                table_type=self.pktype,
                operation=self.operation,
                batch_size=self.batchsize
            )
            args: list[str] = [self.dbfactory.get_char_data(self.pktype, self.operation)] * self.batchsize
            keys: list = []
            with self.dbfactory.get_connection() as conn:
                conn.autocommit = True
                with conn.cursor() as cur:
                    while ops_done < self.operations:
                        logging.debug(f"Worker {self.id} performing {self.operation} for batch size {self.batchsize}")
                        start = asyncio.get_event_loop().time()
                        cur.execute(ins_stmt, args)
                        if self.operation == DBOperation.INSERT:
                            results: list[tuple] = cur.fetchall()
                            keys.extend([row[0] for row in results])
                        end = asyncio.get_event_loop().time()
                        output = pd.concat([output, pd.DataFrame({
                            "workerid": [self.id],
                            "operation": [self.operation.value],
                            "batchsize": [self.batchsize],
                            "duration": [end - start],
                        })], ignore_index=True)
                        ops_done += self.batchsize
            return output  # Placeholder for actual metrics

    def __init__(self,
                 dbfactory: DBFactory,
                 pktype: DBPrimaryKeyType,
                 operation: DBOperation,
                 workers: int,
                 batchsize: int,
                 operations: int
    ) -> None:
        self.dbfactory = dbfactory
        self.pktype = pktype
        self.operation = operation
        self.workers = workers
        self.batchsize = batchsize
        self.operations = operations
    
    async def run_test(self) -> pd.DataFrame:
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
                operation=self.operation,
                batchsize=self.batchsize,
                operations=ops_per_worker,
            ) for i in range(self.workers)
        ]
        
        self.dbfactory.create_table(self.pktype)

        tasks: list[Task[pd.DataFrame]] = []
        async with asyncio.TaskGroup() as tg:
            testers = [w.run() for w in workers]
            for task in testers:
                tasks.append(tg.create_task(task))
        results = await asyncio.gather(*tasks)

        self.dbfactory.drop_table(self.pktype)

        return pd.concat(results, ignore_index=True)