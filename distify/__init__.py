from ray.util.multiprocessing import Pool
import os
import time
import uuid
import sqlite3
from tqdm import tqdm
import logging
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore


def timestamp():
    return time.strftime("%Y-%m-%d-%H%M")


logging.basicConfig(filename='distify' + timestamp() + '.log', level=logging.INFO)


@dataclass
class DistifyConfig:
    checkpoint_frequency: int
    parallel: bool


def register_configs():
    cs = ConfigStore.instance()
    cs.store(
        name="config",
        node=DistifyConfig,
    )


register_configs()


class Globals:
    def __init__(self):
        self.F_MAPPERS = None


G = Globals()


class Mapper:
    def __init__(self):
        self.process_id = os.uname()[1] + '_' + str(os.getpid())
        self.logger = logging.getLogger(self.process_id)
        #logging.basicConfig(filename=self.process_id + '.log', level=logging.INFO)

    def process(self, x) -> None:
        raise NotImplementedError

    def __call__(self, x):
        self.process(x)
        return hash(x)

    def get_unique_path(self):
        ts = timestamp()
        extra_id = uuid.uuid4().hex
        return os.path.join(os.getcwd(), self.process_id + '_' + ts + '_' + extra_id)

    @classmethod
    def mapper_factory(cls, *args):
        return cls(*args)


class Processor:
    def __init__(self, mapper_class, stream, distify_cfg, mapper_args=()):
        self.mapper_class = mapper_class
        self.stream = stream
        self.checkpoint_frequency = distify_cfg.checkpoint_frequency
        self.mapper_args = mapper_args
        self.parallel = distify_cfg.parallel

        self.con = sqlite3.connect('checkpoint.db', check_same_thread=False)  # TODO: Review
        self.cur = self.con.cursor()

        # Create table
        sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS elements (
                                            id integer PRIMARY KEY,
                                            hash integer
                                        );"""
        self.cur.execute(sql_create_tasks_table)
        index_sql = "CREATE INDEX IF NOT EXISTS hash_index ON elements(hash)"

        self.cur.execute(index_sql)

    def done(self, x):
        self.cur.execute(f"SELECT * FROM elements WHERE hash = {x}")
        data = self.cur.fetchall()
        return len(data) != 0

    def run(self):
        work_dir = os.getcwd()

        if self.parallel:
            with Pool(initializer=self._initialize_mappers, initargs=(self.mapper_class.mapper_factory, work_dir,
                                                                      self.mapper_args)) as pool:
                res = pool.imap_unordered(self._map_f, self.stream)
                idx = -1

                for e in tqdm(res, total=len(self.stream)):
                    idx += 1
                    h = hash(e)
                    if self.done(h):
                        continue
                    self.cur.execute(f"INSERT INTO elements VALUES ({h}, {h})")
                    if idx % self.checkpoint_frequency == 0:
                        self.con.commit()
        else:
            self._initialize_mappers(self.mapper_class.mapper_factory, work_dir, self.mapper_args)
            idx = -1
            for e in tqdm.qdm(self.stream):
                idx += 1
                h = hash(e)
                if self.done(h):
                    continue
                self.cur.execute(f"INSERT INTO elements VALUES ({h}, {h})")
                if idx % self.checkpoint_frequency == 0:
                    self.con.commit()
        self.con.close()

    @staticmethod
    def _map_f(x):
        return G.F_MAPPERS(x)

    @staticmethod
    def _initialize_mappers(mapper_factory, work_dir, mapper_args):
        os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory(mapper_args)


__all__ = ['Processor', 'Mapper', 'timestamp']
