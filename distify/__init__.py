from ray.util.multiprocessing import Pool as RayPool
from multiprocessing.pool import ThreadPool as MTPool
import multiprocessing
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
from typing import Optional
try:
    import thread
except ImportError:
    import _thread as thread
from timeout_decorator import timeout
from timeout_decorator.timeout_decorator import TimeoutError

# TODO: fix multi-threaded forks
# Ideally, with set context 'spawn', but Ray doesn't support it?
# from multiprocessing_logging import install_mp_handler
# install_mp_handler()

# TODO: fault tolerance? https://docs.ray.io/en/latest/auto_examples/plot_example-lm.html

SQL_CHECK_SAME_THREAD = False  # TODO: REVIEW
CHECKPOINT_DB_PATH = 'checkpoint.db'


def timestamp():
    return time.strftime("%Y-%m-%d-%H%M")

# TODO: improve logging
# TODO: test multiple backends (mp, ray, threads)
# TODO: typing
# TODO: timeout doesnt work with sleep?
# TODO: Improve logging, interaction with tqdm, etc
# TODO: pip
# TODO: CI
# TODO: testing
# TODO: Slurm


class TqdmLoggingHandler(logging.StreamHandler):
    """Avoid tqdm progress bar interruption by logger's output to console"""
    # see logging.StreamHandler.eval method:
    # https://github.com/python/cpython/blob/d2e2534751fd675c4d5d3adc208bf4fc984da7bf/Lib/logging/__init__.py#L1082-L1091
    # and tqdm.write method:
    # https://github.com/tqdm/tqdm/blob/f86104a1f30c38e6f80bfd8fb16d5fcde1e7749f/tqdm/std.py#L614-L620

    def emit(self, record):
        try:
            tqdm.write('\n', end=self.terminator)
            #  msg = '\n' + self.format(record) + '\n'
            #  tqdm.write(msg, end=self.terminator)
        except RecursionError:
            raise
        except Exception:
            self.handleError(record)


@dataclass
class DistifyConfig:
    # checkpoint_frequency: int
    log_frequency: int
    chunksize: int
    parallel: bool
    # Inside pool?
    # Then SQL connection should be initialized for each node etc
    parallelize_checkpoint_retrieval: bool
    requires_order: bool
    timeout: Optional[int]


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
        self.timeout = None


G = Globals()


def exit_after():
    '''
    use as decorator to exit process if
    function takes longer than s seconds
    '''

    if G.timeout is not None:
        def outer(fn):
            @timeout(G.timeout)
            def inner(*args, **kwargs):
                return fn(*args, **kwargs)

            def inner2(*args, **kwargs):
                try:
                    res = inner(*args, **kwargs)
                except TimeoutError as e:
                    res = {'hash': hash(args[1]), 'result': None}
                return res
            return inner2
        return outer

    def outer(fn):
        return fn
    return outer


class Worker:
    def __init__(self):
        self.process_id = os.uname()[1] + '_' + str(os.getpid())
        self.logger = logging.getLogger(self.process_id)
        self.logger.addHandler(TqdmLoggingHandler())

    def get_unique_path(self):
        ts = timestamp()
        extra_id = uuid.uuid4().hex
        return os.path.join(os.getcwd(), self.process_id + '_' + ts + '_' + extra_id)

    @classmethod
    def factory(cls, *args):
        return cls(*args)


class Mapper(Worker):

    def map(self, x) -> None:
        raise NotImplementedError

    def __call__(self, x):
        # return self.process(x)
        try:
            res = {'hash': hash(x), 'result': self.map(x)}
        except BaseException as e:
            self.logger.warning(f'Uncaught exception: {str(e)}')
            res = {'hash': hash(x), 'result': None}
        return res


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


class MapperComposer(Mapper):
    def __init__(self, mappers, mappers_args):
        self.mappers = [mapper.factory(mapper_args) for mapper, mapper_args in zip(mappers, mappers_args)]
        super().__init__()

    def map(self, x) -> None:
        result = x
        for mapper in self.mappers:
            result = mapper(result)
        return result


class ReducerComposer(Worker):
    def __init__(self, reducers, reducers_args):
        self.reducers = [reducer.factory(reducer_args) for reducer, reducer_args in zip(reducers, reducers_args)]
        super().__init__()

    def reduce(self, store, values):
        ac = store
        for reducer in self.reducers:
            ac = reducer(ac, values)
        return ac

    @property
    def default_value(self):
        return self.reducers[0].default_value


@contextlib.contextmanager
def SingleProcessPool(initializer, initargs):
    initializer(*initargs)

    class NullPool:
        def imap_unordered(self, f, l, chunksize=1):
            return map(f, l)

        def imap_ordered(self, f, l, chunksize=1):
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
            self.stream = sorted(stream)
        self.timeout = distify_cfg.timeout
        # self.checkpoint_frequency = distify_cfg.checkpoint_frequency
        self.mapper_args = mapper_args
        self.parallel_backend = distify_cfg.parallel_backend
        assert self.parallel_backend in ['ray', 'mp', 'mt', 'seq']
        self.reducer_class = reducer_class
        self.reducer_args = reducer_args
        self.distify_cfg = distify_cfg
        self.logger = logging.getLogger('DISTIFY MAIN')
        restoring = os.path.exists(CHECKPOINT_DB_PATH)

        self.con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
        self.cur = self.con.cursor()

        # Checkpoint
        if not restoring:
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
            pool = multiprocessing.get_context('spawn').Pool
        elif self.parallel_backend == 'mt':
            pool = MTPool
        else:
            pool = SingleProcessPool
        result = None

        with pool(initializer=self._initialize, initargs=(self.mapper_class.factory, work_dir,
                                                          self.mapper_args,
                                                          self.distify_cfg.parallelize_checkpoint_retrieval,
                                                          self.reducer_class.factory if self.reducer_class is not None else None,
                                                          self.reducer_args if self.reducer_class is not None else None,
                                                          self.timeout
                                                          )) as p:
            self._initialize(self.mapper_class.factory, work_dir,
                             self.mapper_args,
                             self.distify_cfg.parallelize_checkpoint_retrieval,
                             self.reducer_class.factory if self.reducer_class is not None else None,
                             self.reducer_args if self.reducer_class is not None else None,
                             self.timeout
                             )
            if self.distify_cfg.parallelize_checkpoint_retrieval:
                new_stream = self.filter_pool(p, self.not_done_global, self.stream)
                # TODO: close connection for all nodes except master
                self.con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
                self.cur = self.con.cursor()

            if self.distify_cfg.requires_order:
                new_stream = sorted(new_stream)
                res = p.imap(self._map_f, new_stream, chunksize=self.distify_cfg.chunksize)
            else:
                res = p.imap_unordered(self._map_f, new_stream, chunksize=self.distify_cfg.chunksize)

            if len(new_stream) != len(self.stream):
                self.logger.info(f'Resuming execution from checkpoint {os.getcwd()}')
            pbar = tqdm(res, initial=len(self.stream) - len(new_stream), total=len(self.stream))
            for idx, e in enumerate(pbar):
                if isinstance(e, BaseException):
                    self.logger.info(f'Error in worker: {str(e)}')
                    continue
                h = e['hash']
                result = e['result']
                self.cur.execute(f"INSERT INTO elements VALUES ({h}, {h})")
                # TODO: reintroduce periodic checkpointing
                # if idx % self.log_reduce_frequency == 0:
                # TODO: reduction could (should?) be run in parallel
                if self.reducer_class is not None:
                    self.cur.execute(f"SELECT id, value FROM reduce")
                    current_reduced = self.cur.fetchall()
                    id_, value = current_reduced[0]
                    if value is not None:
                        value = json.loads(value)
                    reduced, log_message = self._reduce_f(value, [result])
                    if log_message is not None and idx % self.distify_cfg.log_frequency == 0:
                        pbar.set_description(log_message)
                        # TODO: Also log log_message, but only to file, not to console
                    reduced_dump = json.dumps(reduced)
                    sql = f''' UPDATE reduce
                                                      SET value = '{reduced_dump}' 
                                                      WHERE id = {id_}'''
                    self.cur.execute(sql)
                    self.con.commit()
        self.con.close()
        if self.reducer_class is not None:
            with open('reduced.json', 'w', encoding='utf-8') as f:
                json.dump(reduced, f, ensure_ascii=False, indent=4)
        return result

    @staticmethod
    def _map_f(x):
        return G.F_MAPPERS(x)

    @staticmethod
    def _reduce_f(store, values):
        return G.F_REDUCERS(store, values)

    @staticmethod
    def _initialize(mapper_factory, work_dir, mapper_args, parallelize_checkpoint_retrieval,
                    reducer_factory, reducer_args, timeout):
        os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory(*mapper_args)
        if reducer_factory is not None:
            G.F_REDUCERS = reducer_factory(*reducer_args)
        if parallelize_checkpoint_retrieval:
            G.sql_con = sqlite3.connect(CHECKPOINT_DB_PATH, check_same_thread=SQL_CHECK_SAME_THREAD)
            G.sql_cur = G.sql_con.cursor()
        G.timeout = timeout


__version__ = '0.3.3'

__all__ = ['Processor', 'Mapper', 'Reducer', '__version__', 'MapperComposer', 'ReducerComposer']
