import os
import hydra
import logging
from omegaconf import DictConfig
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
from pprint import pformat
from distify import Mapper, Processor, Reducer, ReducerResult, MapperResult
from distify.contrib import SimpleCheckpointReducer, ReducerComposer

log = logging.getLogger(__name__)


@dataclass
class MyMapperConfig:
    my_custom_argument: int
    my_other_custom_argument: int


@dataclass
class MyReducerConfig:
    pass


@dataclass
class MyAppConfig:
    mapper: MyMapperConfig
    reducer: MyReducerConfig


def register_configs():
    cs = ConfigStore.instance()
    cs.store(
        group="app",
        name="my_app",
        node=MyAppConfig,
    )


register_configs()


class MyMapper(Mapper):
    def __init__(self, cfg: MyMapperConfig):
        super().__init__()
        self.non_pickable_dependency = lambda x: x + cfg.my_custom_argument + cfg.my_other_custom_argument
        self.write_path = self.get_unique_path() + '.txt'
        self.fd = open(self.write_path, 'a')

    def map(self, x):
        self.fd.write(str(self.non_pickable_dependency(x)) + '\n')
        self.fd.flush()
        # Returning a value is optional! But if we want to use a Reducer, we should return something
        return x


# Reduction is optional
class MyReducer(Reducer):
    def __init__(self, cfg: MyReducerConfig):
        super().__init__()
        self.cfg = cfg

    @property
    def default_value(self):
        return 0

    def reduce(self, store, values: MapperResult):
        result = store + sum(values.input)
        log_message = f'Reduced so far: {result}'
        return ReducerResult(result=result, log_message=log_message)


def generate_dummy_input():
    path = os.path.join(os.path.dirname(__file__), 'dummy_input.txt')
    if not os.path.exists(path):
        with open(path, 'w') as f:
            f.writelines(list(map(lambda x: f'{x}\n', list(range(0, 10_000)))))
    return path


@hydra.main(config_path="conf", config_name="base_config")
def main(cfg: DictConfig) -> None:
    logging.info(pformat(cfg))
    logging.info(os.getcwd())
    # Again, reducer_class and reducer_args arguments are optional!
    # Stream must be list, not generator
    original_input_path = generate_dummy_input()
    if SimpleCheckpointReducer.exists():
        inputs, current_reduced = SimpleCheckpointReducer.load(original_input_path)
    else:
        with open(original_input_path, 'r') as f:
            inputs = f.readlines()
        inputs = list(map(int, inputs))
        current_reduced = None
    processor = Processor(inputs=inputs, current_reduced=current_reduced, mapper_class=MyMapper,
                          mapper_args=[cfg.app.mapper],
                          cfg=cfg.distify, reducer_class=ReducerComposer,  # The checkpoint reducer should always be the last one
                          reducer_args=[[MyReducer, SimpleCheckpointReducer], [[cfg.app.reducer], []]])
    reduced = processor.run()
    assert reduced == sum(list(range(0, 10_000)))
    logging.info('Finished execution correctly')
    logging.info(pformat(reduced))


if __name__ == '__main__':
    main()
