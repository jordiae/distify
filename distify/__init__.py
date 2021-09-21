from ray.util.multiprocessing import Pool as RayPool
from multiprocessing.pool import ThreadPool as MTPool
from multiprocessing.pool import Pool as MPPool
import os
import time
import uuid
import sqlite3
from tqdm import tqdm
import logging
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
import contextlib
import json
import threading
try:
    import thread
except ImportError:
    import _thread as thread

# TODO: fault tolerance? https://docs.ray.io/en/latest/auto_examples/plot_example-lm.html

SQL_CHECK_SAME_THREAD = False  # TODO: REVIEW
CHECKPOINT_DB_PATH = 'checkpoint.db'


def timestamp():
    return time.strftime("%Y-%m-%d-%H%M")

# logging.basicConfig(filename='distify' + timestamp() + '.log', level=logging.INFO)

# TODO: improve logging
# TODO: test multiple backends (mp, ray, threads)
# TODO: typing
# TODO: exit after and sleep?
# TODO: Improve logging, interaction with tqdm, etc

@dataclass
class DistifyConfig:
    checkpoint_frequency: int
    parallel: bool
    # Inside pool?
    # Then SQL connection should be initialized for each node etc
    parallelize_checkpoint_retrieval: bool
    requires_order: bool


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
        self.F_REDUCERS = None
        self.sql_con = None
        self.sql_cur = None
        self.exit_after_seconds = None


G = Globals()

# Credits: https://stackoverflow.com/questions/492519/timeout-on-a-function-call


def quit_function(fn_name):
    # sys.stderr.flush()
    thread.interrupt_main()


def exit_after():
    '''
    use as decorator to exit process if
    function takes longer than s seconds
    '''
    def outer(fn):
        def inner(*args, **kwargs):
            if G.exit_after_seconds is not None:
                timer = threading.Timer(G.exit_after_seconds, quit_function, args=[fn.__name__])
                timer.start()
                try:
                    result = fn(*args, **kwargs)
                except:
                    result = {'hash': hash(args[1]), 'result': None}
                finally:
                    timer.cancel()
                return result
            else:
                return fn(*args, **kwargs)
        return inner
    return outer


class Worker:
    def __init__(self):
        self.process_id = os.uname()[1] + '_' + str(os.getpid())
        self.logger = logging.getLogger(self.process_id)
        # logging.basicConfig(filename=self.process_id + '.log', level=logging.INFO)

    def get_unique_path(self):
        ts = timestamp()
        extra_id = uuid.uuid4().hex
        return os.path.join(os.getcwd(), self.process_id + '_' + ts + '_' + extra_id)

    @classmethod
    def factory(cls, *args):
        return cls(*args)


class Mapper(Worker):
    def __init__(self, exit_after_seconds = None):
        super().__init__()
        self.exit_after_seconds = exit_after_seconds

    def map(self, x) -> None:
        raise NotImplementedError

    @exit_after()
    def __call__(self, x):
        # return self.process(x)
        return {'hash': hash(x), 'result': self.map(x)}


class Reducer(Worker):

    def reduce(self, store, values):
        raise NotImplementedError

    @property
    def default_value(self):
        raise NotImplementedError

    def __call__(self, store, values):
        if store is None:
            store = self.default_value
        values = list(filter(lambda x: x is not None, values))
        if len(values) == 0:
            values = [self.default_value]
        return self.reduce(store, values)


@contextlib.contextmanager
def SingleProcessPool(initializer, initargs):
    initializer(*initargs)

    class NullPool:
        def imap_unordered(self, f, l):
            return map(f, l)

        def imap_ordered(self, f, l):
            return map(f, l)

        def map(self, f, l):
            return list(map(f, l))

    yield NullPool()

    # TODO: Close?


class Processor:
    def __init__(self, mapper_class, stream, distify_cfg, mapper_args=(), reducer_class=None, reducer_args=()):
        self.mapper_class = mapper_class
        self.stream = stream
        if distify_cfg.requires_order:
            self.stream = sorted(list)
        self.exit_after_seconds = distify_cfg.exit_after_seconds
        self.checkpoint_frequency = distify_cfg.checkpoint_frequency
        self.mapper_args = mapper_args
        self.parallel_backend = distify_cfg.parallel_backend
        assert self.parallel_backend in ['ray', 'mp', 'mt', 'seq']
        self.reducer_class = reducer_class
        self.reducer_args = reducer_args
        self.distify_cfg = distify_cfg

        self.con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
        self.cur = self.con.cursor()

        # Checkpoint
        sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS elements (
                                            id integer PRIMARY KEY,
                                            hash integer
                                        );"""
        self.cur.execute(sql_create_tasks_table)
        index_sql = "CREATE INDEX IF NOT EXISTS hash_index ON elements(hash)"
        self.cur.execute(index_sql)

        # Reduced
        if reducer_class is not None:
            sql_create_tasks_table = """CREATE TABLE IF NOT EXISTS reduce (
                                                        id integer PRIMARY KEY,
                                                        value text
                                                    );"""
            self.cur.execute(sql_create_tasks_table)
            self.cur.execute(f"INSERT INTO reduce VALUES (0, {json.dumps(None)})")

        self.con.commit()
        if self.distify_cfg.parallelize_checkpoint_retrieval:
            self.con.close()
            del self.cur
            del self.con

    @staticmethod
    def filter_pool(pool, func, iterable):
        res = pool.map(func, iterable)
        to_keep = []
        for element, keep in zip(iterable, res):
            if keep:
                to_keep.append(element)
        return to_keep

    @staticmethod
    def done_global(x):
        x = hash(x)
        G.sql_cur.execute(f"SELECT * FROM elements WHERE hash = {x}")
        data = G.sql_cur.fetchall()
        return len(data) != 0

    def done(self, x):
        x = hash(x)
        self.cur.execute(f"SELECT * FROM elements WHERE hash = {x}")
        data = self.cur.fetchall()
        return len(data) != 0

    def not_done(self, x):
        return not self.done(x)

    def not_done_global(self, x):
        return not self.done_global(x)

    def run(self):
        work_dir = os.getcwd()

        if not self.distify_cfg.parallelize_checkpoint_retrieval:
            new_stream = list(filter(self.not_done, self.stream))
        if self.parallel_backend == 'ray':
            pool = RayPool
        elif self.parallel_backend == 'mp':
            pool = MPPool
        elif self.parallel_backend == 'mt':
            pool = MTPool
        else:
            pool = SingleProcessPool
        results = []
        return_value = None
        with pool(initializer=self._initialize, initargs=(self.mapper_class.factory, work_dir,
                                                          self.mapper_args,
                                                          self.distify_cfg.parallelize_checkpoint_retrieval,
                                                          self.reducer_class.factory,
                                                          self.reducer_args,
                                                          self.exit_after_seconds
                                                          )) as p:

            if self.distify_cfg.parallelize_checkpoint_retrieval:
                new_stream = self.filter_pool(p, self.not_done_global, self.stream)
                # TODO: close connection for all nodes except master
                self.con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
                self.cur = self.con.cursor()

            if self.distify_cfg.requires_order:
                new_stream = sorted(new_stream)
                res = p.imap(self._map_f, new_stream)
            else:
                res = p.imap_unordered(self._map_f, new_stream)

            for idx, e in enumerate(tqdm(res, initial=len(self.stream) - len(new_stream), total=len(new_stream))):
                h = e['hash']
                result = e['result']
                results.append(result)
                self.cur.execute(f"INSERT INTO elements VALUES ({h}, {h})")
                if idx % self.checkpoint_frequency == 0:
                    # TODO: reduction could (should?) be run in parallel
                    if self.reducer_class is not None:
                        self.cur.execute(f"SELECT id, value FROM reduce")
                        current_reduced = self.cur.fetchall()
                        id_, value = current_reduced[0]
                        if value is not None:
                            value = json.loads(value)
                        reduced = self._reduce_f(value, results)
                        results = []
                        reduced_dump = json.dumps(reduced)
                        sql = f''' UPDATE reduce
                                      SET value = {reduced_dump} 
                                      WHERE id = {id_}'''
                        self.cur.execute(sql)
                    self.con.commit()
            if self.reducer_class is not None and len(results) > 0:
                self.cur.execute(f"SELECT id, value FROM reduce")
                current_reduced = self.cur.fetchall()
                current_reduced = current_reduced[0]
                id_, value = current_reduced
                if value is not None:
                    value = json.loads(value)
                reduced = self._reduce_f(value, results)
                del results
                reduced_dump = json.dumps(reduced)
                sql = f''' UPDATE reduce
                                                      SET value = {reduced_dump} 
                                                      WHERE id = {id_}'''
                self.cur.execute(sql)
                self.con.commit()
                return_value = reduced
        self.con.close()
        with open('reduced.json', 'w', encoding='utf-8') as f:
            json.dump(reduced, f, ensure_ascii=False, indent=4)
        return return_value

    @staticmethod
    def _map_f(x):
        return G.F_MAPPERS(x)

    @staticmethod
    def _reduce_f(store, values):
        return G.F_REDUCERS(store, values)

    @staticmethod
    def _initialize(mapper_factory, work_dir, mapper_args, parallelize_checkpoint_retrieval,
                    reducer_factory, reducer_args, exit_after_seconds):
        os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory(*mapper_args)
        if reducer_factory is not None:
            G.F_REDUCERS = reducer_factory(*reducer_args)
        if parallelize_checkpoint_retrieval:
            G.sql_con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
            G.sql_cur = G.sql_con.cursor()
        G.exit_after_seconds = exit_after_seconds


__all__ = ['Processor', 'Mapper', 'Reducer']
