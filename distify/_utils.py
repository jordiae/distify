import contextlib
from typing import Callable


class _DummySyncResult:
    def __init__(self, result):
        self.result = result

    def get(self, timeout):
        return self.result


class _NullPool:
    def __init__(self, processes=1):
        pass

    def imap_unordered(self, f, l, chunksize=1):
        return map(f, l)

    def imap_ordered(self, f, l, chunksize=1):
        return map(f, l)

    def map(self, f, l):
        return list(map(f, l))

    def apply_async(self, f, x):
        return _DummySyncResult(f(*x))

    def close(self):
        pass


def _SingleProcessPoolWithoutContextManager(initializer, initargs):
    initializer(*initargs)
    return _NullPool()


@contextlib.contextmanager
def _SingleProcessPoolContextManager(initializer, initargs):
    initializer(*initargs)

    yield _NullPool()

    # TODO: Close?


class _MapperWrapper:
    def __init__(self, mapper):
        self.mapper = mapper

    def __call__(self, x):
        orig_idx, orig_input = x[0]
        error = None
        try:
            output = self.mapper(orig_input)
        except BaseException as e:
            output = None
            error = e
        return orig_idx, orig_input, output, error

    @classmethod
    def get_factory(cls, mapper):
        return _MapperFactory(cls, mapper)
        '''
        if isinstance(mapper, type):
            def _inner(mapper_args):
                return cls(mapper(*mapper_args))
            return _inner
        else:
            assert isinstance(mapper, Callable)
            def _inner(mapper_args):
                return cls(mapper)
            return _inner
        '''

class _MapperFactory:
    def __init__(self, wrapper_class, mapper):
        self._wrapper_class = wrapper_class
        self._mapper = mapper

    def __call__(self, mapper_args):
        if isinstance(self._mapper, type):
            return self._wrapper_class(self._mapper(*mapper_args))
        else:
            assert isinstance(self._mapper, Callable)
            return self._wrapper_class(self._mapper)
