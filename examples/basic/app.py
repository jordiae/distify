from distify import dmap
from distify.contrib import CheckpointMask
import os
import hydra
import logging
from omegaconf import DictConfig
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
from ordered_set import OrderedSet
import distify
from pprint import pformat
from omegaconf import OmegaConf

log = logging.getLogger(__name__)

DUMMY_INPUT = list(range(0, 100_000))


@dataclass
class MyMapperConfig:
    my_custom_argument: int
    my_other_custom_argument: int


@dataclass
class MyAppConfig:
    mapper: MyMapperConfig


def register_configs():
    cs = ConfigStore.instance()
    cs.store(
        group="app",
        name="my_app",
        node=MyAppConfig,
    )


register_configs()


def generate_dummy_input():
    path = os.path.join(os.path.dirname(__file__), 'dummy_input.txt')
    with open(path, 'w') as f:
        f.writelines(list(map(lambda x: f'{x}\n', DUMMY_INPUT)))
    return path


def load_inputs(path):
    with open(path, 'r') as f:
        inputs = f.readlines()
    inputs = list(map(int, inputs))
    return inputs


def my_reduce(iterable, initializer=0):
    return sum(iterable) + initializer


class MyMapper:
    def __init__(self, cfg: MyMapperConfig):
        super().__init__()
        self.non_pickable_dependency = lambda x: x + cfg.my_custom_argument + cfg.my_other_custom_argument
        self.write_path = distify.contrib.get_process_unique_path() + '.txt'
        self.fd = open(self.write_path, 'a')

    def __call__(self, x):
        self.fd.write(str(self.non_pickable_dependency(x)) + '\n')
        self.fd.flush()
        # Returning a value is optional! But if we want to use a Reducer, we should return something
        return x


@hydra.main(config_path="conf", config_name="base_config")
def main(cfg: DictConfig) -> None:
    # cfg = OmegaConf.to_object(cfg)
    logging.info(pformat(cfg))
    logging.info(f'hydra.run.dir={os.getcwd()}')
    input_path = generate_dummy_input()
    inputs = OrderedSet(load_inputs(input_path))
    checkpoint = CheckpointMask.load('.') if CheckpointMask.exists('.') else CheckpointMask('.', inputs=inputs)
    reduced = 0
    work_done = []
    for new_idx, orig_idx, orig_input, output, error, pbar in dmap(inputs=checkpoint.inputs_to_do,
                                                                   inputs_done=checkpoint.inputs_done,
                                                                   mapper=MyMapper,
                                                                   mapper_args=[cfg.app.mapper],
                                                                   chunksize=cfg.distify.chunksize,
                                                                   par_backend=cfg.distify.par_backend,
                                                                   mp_context=cfg.distify.mp_context,
                                                                   # ray_init_args=(...)
                                                                   ):
        if error:
            pbar.set_description('Error')
        else:
            reduced = my_reduce(iterable=[output], initializer=reduced)
            pbar.set_description('OK')

        work_done.append(orig_input)

        if (new_idx + 1) % 1000 == 0:
            for orig_input in work_done:
                checkpoint.mark_as_done(orig_input)
            checkpoint.set_reduced(reduced)

    for orig_input in work_done:
        checkpoint.mark_as_done(orig_input)
    checkpoint.set_reduced(reduced)

    print(reduced, sum(DUMMY_INPUT))
    assert reduced == sum(DUMMY_INPUT)
    logging.info('Finished execution correctly')


if __name__ == '__main__':
    main()
