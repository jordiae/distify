from distify import Mapper, Processor, timestamp
import hydra
import logging
from omegaconf import DictConfig
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
from pprint import pprint

log = logging.getLogger(__name__ )


@dataclass
class MyMapperConfig:
    my_custom_argument: int
    my_other_custom_argument: int


def register_configs():
    cs = ConfigStore.instance()
    cs.store(
        group="app",
        name="my_app",
        node=MyMapperConfig,
    )


register_configs()


def my_stream():
    # Must be list, not generator
    return list(range(0, 20_000))


class MyMapper(Mapper):
    def __init__(self, cfg: MyMapperConfig):
        super().__init__()
        self.non_pickable_dependency = lambda x: x + cfg.my_custom_argument + cfg.my_other_custom_argument
        self.write_path = self.get_unique_path() + '.txt'
        self.fd = open(self.write_path, 'a')

    def process(self, x):
        if x % 10 == 0:
            self.logger.info(f'Hi {x}')  # TODO: Improve logging, interaction with tqdm, etc
        self.fd.write(str(self.non_pickable_dependency(x)) + '\n')
        self.fd.flush()


@hydra.main(config_path="conf", config_name="base_config")
def main(cfg: DictConfig) -> None:
    pprint(cfg)
    processor = Processor(stream=list(range(0, 20_000)), mapper_class=MyMapper, mapper_args=cfg.app,
                          distify_cfg=cfg.distify)
    processor.run()


if __name__ == '__main__':
    main()
