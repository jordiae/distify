import os
import hydra
import logging
from omegaconf import DictConfig
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
from pprint import pformat
from distify import Mapper, Processor, Reducer

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
        if x % 10000 == 0:
            self.logger.info(f'Hi {x}')
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

    def reduce(self, store, values):
        result = store + sum(values)
        log_message = f'Reduced so far: {result}'
        return result, log_message


@hydra.main(config_path="conf", config_name="base_config")
def main(cfg: DictConfig) -> None:
    logging.info(pformat(cfg))
    logging.info(os.getcwd())
    # Again, reducer_class and reducer_args arguments are optional!
    # Stream must be list, not generator
    processor = Processor(stream=list(range(0, 20_000)), mapper_class=MyMapper, mapper_args=[cfg.app.mapper],
                          distify_cfg=cfg.distify, reducer_class=MyReducer, reducer_args=[cfg.app.reducer])
    reduced = processor.run()
    logging.info('Finished execution correctly')
    logging.info(pformat(reduced))


if __name__ == '__main__':
    main()
