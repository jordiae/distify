from parallel import Mapper, Processor
import logging
import hydra
from omegaconf import DictConfig

log = logging.getLogger(__name__)


def my_stream():
    return range(0, 20_000)


class MyMapper(Mapper):
    def __init__(self):
        super().__init__()
        self.non_pickable_dependency = lambda x: x+1
        self.write_path = self.get_unique_path() + '.txt'
        self.fd = open(self.write_path, 'a')

    def process(self, x):
        self.fd.write(str(self.non_pickable_dependency(x)) + '\n')
        self.fd.flush()


@hydra.main(config_path="conf", config_name="config")
def main(cfg: DictConfig) -> None:
    processor = Processor(stream=my_stream(), mapper_class=MyMapper, checkpoint_frequency=cfg.checkpoint_frequency)
    processor.run()


if __name__ == '__main__':
    main()
