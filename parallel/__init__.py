from multiprocessing import Pool
import os
import ray
import time
import uuid
import sqlite3
from tqdm import tqdm
import logging

log = logging.getLogger(__name__)


class Globals:
    def __init__(self):
        self.F_MAPPERS = None


G = Globals()


class Mapper:
    def __init__(self):
        self.process_id = os.uname()[1] + '_' + str(os.getpid())

    def process(self, x) -> None:
        raise NotImplementedError

    @ray.remote
    def log(self, x):
        log.info(x)

    def __call__(self, x):
        self.process(x)
        return hash(x)

    def get_unique_path(self):
        timestamp = time.strftime("%Y-%m-%d-%H%M")
        extra_id = uuid.uuid4().hex
        return os.path.join(os.getcwd(), self.process_id + '_' + timestamp + '_' + extra_id)

    @classmethod
    def mapper_factory(cls, *args, **kwargs):
        return cls(*args, **kwargs)


class Processor:
    def __init__(self, mapper_class, stream, checkpoint_frequency):
        self.mapper_class = mapper_class
        self.stream = stream
        self.checkpoint_frequency = checkpoint_frequency

        self.con = sqlite3.connect('checkpoint.db', check_same_thread=False)
        self.cur = self.con.cursor()

        # Create table
        sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS elements (
                                            id integer PRIMARY KEY,
                                            hash integer
                                        );"""
        self.cur.execute(sql_create_tasks_table)
        index_sql = "CREATE INDEX IF NOT EXISTS hash_index ON elements(hash)"

        self.cur.execute(index_sql)

    def run_stream(self):
        idx = -1
        for e in tqdm(self.stream):
            idx += 1
            h = hash(e)
            if self.done(h):
                continue
            self.cur.execute(f"INSERT INTO elements VALUES ({h}, {h})")
            if idx % self.checkpoint_frequency == 0:
                self.con.commit()
            yield e

    def done(self, x):
        self.cur.execute(f"SELECT * FROM elements WHERE hash = {x}")
        data = self.cur.fetchall()
        return len(data) != 0

    def run(self):
        work_dir = os.getcwd()
        with Pool(initializer=self._initialize_mappers, initargs=(self.mapper_class.mapper_factory, work_dir)) as pool:
            res = pool.imap_unordered(self._map_f, self.run_stream())
            for _ in res:
                pass
        self.con.close()

    @staticmethod
    def _map_f(x):
        return G.F_MAPPERS(x)

    @staticmethod
    def _initialize_mappers(mapper_factory, work_dir=None):
        if work_dir is not None:
            os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory()


__all__ = ['Processor', 'Mapper']
