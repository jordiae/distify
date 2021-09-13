from parallel import Mapper, Processor

import logging

import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def my_stream():
    return range(0, 100)


class MyMapper(Mapper):
    def __init__(self):
        super().__init__()
        self.non_pickable_dependency = lambda x: x+1
        self.fd = open(self.get_unique_path() + '.txt', 'w')

    def process(self, x):
        self.fd.write(str(self.non_pickable_dependency(x)) + '\n')
        self.fd.flush()

from hydra import compose
from hydra.core.config_store import ConfigStore
from hydra.core.hydra_config import HydraConfig
from hydra.utils import to_absolute_path
from omegaconf import OmegaConf
import os

def run(frequency):
    processor = Processor(stream=my_stream(), mapper_class=MyMapper, frequency=frequency)
    processor.run()
@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    #log.info(f"Executing task {cfg.task}")

    if cfg.checkpoint is not None:
        print('Resuming')
        output_dir = to_absolute_path(cfg.checkpoint)
        original_overrides = OmegaConf.load(os.path.join(output_dir, ".hydra", "overrides.yaml"))
        #current_overrides = HydraConfig.get().overrides.task

        hydra_config = OmegaConf.load(os.path.join(output_dir, ".hydra", "hydra.yaml"))
        # getting the config name from the previous job.
        config_name = hydra_config.hydra.job.config_name
        # concatenating the original overrides with the current overrides
        overrides = original_overrides# + current_overrides
        # compose a new config from scratch
        cfg = compose(config_name, overrides=overrides)
    run(frequency=cfg.frequency)


if __name__ == '__main__':
    main()
