from ray.util.multiprocessing import Pool as RayPool
import ray
import psutil
from multiprocessing.pool import ThreadPool as MTPool
import multiprocessing
import os
import time
import uuid
from tqdm import tqdm
import logging
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
import contextlib
from typing import Optional, Tuple, List, TypeVar, Any
from enum import Enum
from ordered_set import OrderedSet

# TODO: fix multi-threaded forks
# Ideally, with set context 'spawn', but Ray doesn't support it?
# from multiprocessing_logging import install_mp_handler
# install_mp_handler()

# TODO: fault tolerance? https://docs.ray.io/en/latest/auto_examples/plot_example-lm.html

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


class ParallelBackend(str, Enum):
    SEQ = 'seq'
    MP = 'mp'
    MT = 'mt'
    RAY = 'ray'


@dataclass
class MapperResult:
    result: Optional[List]
    error: Optional[BaseException]
    idx: List[int]
    input: List


@dataclass
class DistifyConfig:
    parallel_backend: ParallelBackend
    chunksize: int
    timeout1: int
    timeout2: int
    mp_context: str
    max_tasks: int

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


class Worker:
    def __init__(self):
        self.process_id = os.uname()[1] + '_' + str(os.getpid())

    def get_unique_path(self):
        ts = timestamp()
        extra_id = uuid.uuid4().hex
        return os.path.join(os.getcwd(), self.process_id + '_' + ts + '_' + extra_id)

    @classmethod
    def factory(cls, args):
        return cls(*args)


T = TypeVar('T')


class Mapper(Worker):

    def map(self, x: T) -> None:
        raise NotImplementedError

    def __call__(self, chunk: List[Tuple[int, T]]) -> MapperResult:
        #if len(chunk) == 1:
        #    chunk = chunk[0]
        chunk = list(zip(*chunk))
        idxs = chunk[0]
        chunk = chunk[1]
        try:
            res = MapperResult(result=[self.map(element) for element in chunk], error=None, idx=idxs, input=[element for element in chunk])
        except BaseException as e:
            res = MapperResult(result=None, error=e, idx=idxs, input=[element for element in chunk])
        return res


@dataclass
class ReducerResult:
    result: Any
    log_message: Optional[str]


class Reducer(Worker):

    def reduce(self, store, values: List[MapperResult]) -> ReducerResult:
        raise NotImplementedError
    
    @property
    def default_value(self):
        raise NotImplementedError

    def __call__(self, store, values):
        if store is None:
            store = self.default_value
        values = list(filter(lambda x: x is not None, values.result))
        if len(values) == 0:
            values = [self.default_value]
        return self.reduce(store, values)


class DummySyncResult:
    def __init__(self, result):
        self.result = result

    def get(self, timeout):
        return self.result

@contextlib.contextmanager
def SingleProcessPool(initializer, initargs):
    initializer(*initargs)

    class NullPool:
        def __init__(self, processes=1):
            pass

        def imap_unordered(self, f, l, chunksize=1):
            return map(f, l)

        def imap_ordered(self, f, l, chunksize=1):
            return map(f, l)

        def map(self, f, l):
            return list(map(f, l))

        def apply_async(self, f, x):
            return DummySyncResult(f(x))

    yield NullPool()

    # TODO: Close?



class Processor:
    def __init__(self, cfg: DistifyConfig, inputs: OrderedSet, mapper_class, mapper_args=(), reducer_class=None, reducer_args=(),
                 current_reduced: Optional = None):
        self.cfg = cfg
        self.inputs = inputs
        self.mapper_class = mapper_class
        self.mapper_args = mapper_args
        self.reducer_class = reducer_class
        self.reducer_args = reducer_args
        self.logger = logging.getLogger('DISTIFY MAIN')
        self.current_reduced = current_reduced
        self.reduce = None

    def get_n_cpus(self):
        if self.cfg.parallel_backend == ParallelBackend.SEQ:
            return int(ray.available_resources()['CPU'])
        elif self.cfg.parallel_backend == ParallelBackend.MP:
            return multiprocessing.cpu_count()
        elif self.cfg.parallel_backend == ParallelBackend.MT:
            return psutil.cpu_count(logical=True)//psutil.cpu_count(logical=False)
        else:
            return 1

    def get_pool(self):
        if self.cfg.parallel_backend == ParallelBackend.RAY:
            pool = RayPool
        elif self.cfg.parallel_backend == ParallelBackend.MP:
            pool = multiprocessing.get_context(self.cfg.mp_context).Pool  # spawn
        elif self.cfg.parallel_backend == ParallelBackend.MT:
            pool = MTPool
        else:
            pool = SingleProcessPool
        return pool

    def process_mapper_result(self, mapper_result, pbar, current_reduced, iteration):
        if isinstance(mapper_result, BaseException):
            self.logger.info(f'Error in mapper: {str(mapper_result)}')
            reduced = None
        elif isinstance(mapper_result.error, BaseException):
            self.logger.info(f'Error in mapper: {str(mapper_result.error)}')
            reduced = None
        elif isinstance(mapper_result, MapperResult):
            idx = mapper_result.idx
            work_done = len(idx)
            pbar.update(work_done)
            for i in idx:
                self.not_done_list[i] = False

            if self.reducer_class:
                reduce_result = self._reduce_f(current_reduced, mapper_result)
                reduced = reduce_result.result
                log_message = reduce_result.log_message
                if log_message:
                    pbar.set_description(log_message)
                    # TODO: Also log log_message, but only to file, not to console
        else:
            raise RuntimeError('Unknown mapper_result type')
        return reduced

    def run(self):
        work_dir = os.getcwd()

        self.not_done_list = [True for _ in self.inputs]

        initial = len(self.inputs) - sum(self.not_done_list)
        total = len(self.inputs)

        pool = self.get_pool()

        nproc = self.get_n_cpus()
        assert self.cfg.max_tasks >= nproc

        n_not_done = sum(self.not_done_list)
        with tqdm(initial=initial, total=total) as pbar:
            while n_not_done > 0:
                n_not_done = sum(self.not_done_list)
                self.logger.info(f"Restarting Pool. {n_not_done} elements to go.")
                # print(threading.active_count())
                with pool(initializer=self._initialize, initargs=(self.mapper_class.factory, work_dir,
                                                                  self.mapper_args,
                                                                  )) as p:
                    
                    if not self.reduce and self.reducer_class:
                        self.reduce = self.reducer_class.factory(self.reducer_args)
                    chunks = []
                    current_chunk = []
                    for i in range(len(self.inputs)):
                        if len(current_chunk) == self.cfg.chunksize:
                            chunks.append(current_chunk)
                            current_chunk = []
                            if len(chunks) == self.cfg.max_tasks:
                                break
                        if self.not_done_list[i]:
                            current_chunk.append((i, self.inputs[i]))
                    if len(current_chunk) > 0:
                        chunks.append(current_chunk)

                    tasks = [p.apply_async(self._map_f, chunks[i]) for i in range(len(chunks))]

                    failed = []
                    for iteration, task in enumerate(tasks):
                        try:
                            mapper_result = task.get(timeout=self.cfg.timeout1)
                            self.current_reduced = self.process_mapper_result(mapper_result, pbar, iteration=iteration,
                                                                            current_reduced=self.current_reduced)
                        except TimeoutError:
                            failed.append(task)
                            for task in failed:
                                try:
                                    mapper_result = task.get(timeout=self.cfg.timeout2)
                                    self.current_reduced = self.process_mapper_result(mapper_result, pbar, iteration=iteration,
                                                                                    current_reduced=self.current_reduced)
                                except TimeoutError:
                                    continue

                            if len(failed) < nproc:
                                self.logger.info(f"{len(failed)} processes are blocked, "
                                                 f"but {nproc - len(failed)} remain.")
                            else:
                                break
        return self.current_reduced

    @staticmethod
    def _map_f(args):
        return G.F_MAPPERS(args)

    def _reduce_f(self, store, values):
        return self.reduce(store, values)

    @staticmethod
    def _initialize(mapper_factory, work_dir, mapper_args):
        os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory(mapper_args)


# TODO: if Ray, add working directory to path
