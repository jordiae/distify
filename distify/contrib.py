from ._core import Reducer, ReducerResult, MapperResult, Mapper
from ordered_set import OrderedSet
import json
import os


class SimpleCheckpointReducer(Reducer):
    TXT_FILENAME = 'checkpoint.txt'
    JSON_FILENAME = 'reduced_checkpoint.json'

    def __init__(self):
        super().__init__()
        self.input_fd = open(SimpleCheckpointReducer.TXT_FILENAME, 'a')
        self.reduced_fd = open(SimpleCheckpointReducer.JSON_FILENAME, 'w')

    def reduce(self, store, values: MapperResult):
        self.input_fd.writelines([str(input_) for input_ in values.input])
        json.dump(store, self.reduced_fd)
        return ReducerResult(result=store, log_message=None)

    @property
    def default_value(self):
        return None

    @staticmethod
    def exists():
        return os.path.exists(SimpleCheckpointReducer.TXT_FILENAME)

    @staticmethod
    def load(original_path):
        with open(original_path, 'r') as original, open(SimpleCheckpointReducer.TXT_FILENAME, 'r') as checkpoint:
            original = OrderedSet(original.readlines())
            checkpoint = OrderedSet(checkpoint.readlines())
        inputs = original - checkpoint
        with open(SimpleCheckpointReducer.JSON_FILENAME) as f:
            reduced = json.load(f)
        return inputs, reduced


class MapperComposer(Mapper):
    def __init__(self, mappers, mappers_args):
        self.mappers = [mapper.factory(mapper_args) for mapper, mapper_args in zip(mappers, mappers_args)]
        super().__init__()

    def map(self, x) -> None:
        result = x
        for mapper in self.mappers:
            result = mapper(result)
        return result


class ReducerComposer(Reducer):
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
