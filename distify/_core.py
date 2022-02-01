from ray.util.multiprocessing import Pool as RayPool
import ray
import psutil
from multiprocessing.pool import ThreadPool as MTPool
import multiprocessing
import os
from tqdm import tqdm
from typing import Optional, Callable, Iterable, Union
from enum import Enum
from ordered_set import OrderedSet
from distify._utils import _SingleProcessPoolContextManager, _SingleProcessPoolWithoutContextManager, _MapperWrapper

# TODO: fix multi-threaded forks
# Ideally, with set context 'spawn', but Ray doesn't support it?
# from multiprocessing_logging import install_mp_handler
# install_mp_handler()

# TODO: fault tolerance? https://docs.ray.io/en/latest/auto_examples/plot_example-lm.html


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


class Globals:
    def __init__(self):
        self.F_MAPPERS = None


G = Globals()


class dmap:
    def __init__(self, inputs: OrderedSet, inputs_done: OrderedSet, mapper: Union[type, Callable],
                 mapper_args: Optional[Iterable] = None, chunksize=1,
                 par_backend='seq', mp_context='fork', ray_init_kwargs=None):

        self.par_backend = par_backend
        self.mp_context = mp_context
        self.ray_init_kwargs = ray_init_kwargs

        initial_bar_len = len(inputs_done)
        total_bar_len = len(inputs)

        inputs_to_do = inputs - inputs_done

        work_dir = os.getcwd()

        PoolClass = self._get_pool_class(context_manager=False)

        # nproc = self._get_n_cpus()

        self.pool = PoolClass(initializer=self._initialize, initargs=(_MapperWrapper.get_factory(mapper),
                                                                 work_dir, mapper_args,))

        res = self.pool.imap_unordered(self._map_f, [(idx, inp) for idx, inp in enumerate(inputs_to_do)],
                                       chunksize=chunksize)
        self.pbar = tqdm(res, initial=initial_bar_len, total=total_bar_len)

    def __iter__(self):

        try:
            for new_idx, (orig_idx, orig_input, output, error) in enumerate(self._process_result(self.pbar)):
                work_done = 1
                self.pbar.update(work_done)
                yield new_idx, orig_idx, orig_input, output, error, self.pbar
        finally:
            self.pool.close()

    def _process_result(self, iter):
        for e in iter:
            if isinstance(e, BaseException):
                orig_idx = None
                orig_input = None
                output = None
                error = e
                yield orig_idx, orig_input, output, error
            else:
                yield e

    def _get_n_cpus(self):
        if self.par_backend == ParallelBackend.RAY:
            return int(ray.available_resources()['CPU'])
        elif self.par_backend == ParallelBackend.MP:
            return multiprocessing.cpu_count()
        elif self.par_backend == ParallelBackend.MT:
            return psutil.cpu_count(logical=True)//psutil.cpu_count(logical=False)
        else:
            return 1

    def _get_pool_class(self, context_manager=True):
        if self.par_backend == ParallelBackend.RAY:
            if not ray.is_initialized():
                if self.ray_init_kwargs:
                    ray.init(**self.ray_init_kwargs)
                else:
                    ray.init(address='auto')
            pool = RayPool
        elif self.par_backend == ParallelBackend.MP:
            pool = multiprocessing.get_context(self.mp_context).Pool  # spawn
        elif self.par_backend == ParallelBackend.MT:
            pool = MTPool
        else:
            pool = _SingleProcessPoolContextManager if context_manager else _SingleProcessPoolWithoutContextManager
        return pool

    @staticmethod
    def _map_f(*args):
        return G.F_MAPPERS(args)

    @staticmethod
    def _initialize(mapper_factory, work_dir, mapper_args):
        os.chdir(work_dir)  # needed for ray
        G.F_MAPPERS = mapper_factory(mapper_args)

