import os
import hydra
import logging
from omegaconf import DictConfig
from dataclasses import dataclass
from hydra.core.config_store import ConfigStore
from pprint import pformat
from distify import Mapper, Processor
from ordered_set import OrderedSet
from distify.contrib import Checkpoint, Reducer

log = logging.getLogger(__name__)

DUMMY_INPUT = list(range(0, 10_000))


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


def generate_dummy_input():
    path = os.path.join(os.path.dirname(__file__), 'dummy_input.txt')
    if not os.path.exists(path):
        with open(path, 'w') as f:
            f.writelines(list(map(lambda x: f'{x}\n', DUMMY_INPUT)))
    return path


def load_inputs(path):
    with open(path, 'r') as f:
        inputs = f.readlines()
    inputs = list(map(int, inputs))
    return inputs


class MyReducer(Reducer):
    def reduce(self, item, store):
        return item + store

    @property
    def default_value(self):
        return 0


@hydra.main(config_path="conf", config_name="base_config")
def main(cfg: DictConfig) -> None:
    logging.info(pformat(cfg))
    logging.info(os.getcwd())
    # Again, reducer_class and reducer_args arguments are optional. Stream must be list, not generator
    original_input_path = generate_dummy_input()
    reducer = MyReducer()
    if Checkpoint.exists(os.getcwd()):
        logging.info(f'Resuming from checkpoint: {os.getcwd()}')
        checkpoint = Checkpoint(path=os.getcwd())
        logging.info(f'Last processed item was {checkpoint.data["done"][-1]}')
        items_to_do = checkpoint.get_not_done_items()
        reduced = checkpoint.get_reduced()
    else:
        logging.info(f'Starting execution from scratch in: {os.getcwd()}')
        inputs = OrderedSet(load_inputs(original_input_path))
        checkpoint = Checkpoint(path=os.getcwd(), all_items=inputs)
        items_to_do = inputs
        reduced = reducer.default_value
    for processed_mapper_result, pbar in Processor(inputs=items_to_do, mapper_class=MyMapper,
                                                   mapper_args=[cfg.app.mapper], cfg=cfg.distify,
                                                   initial_bar=checkpoint.get_n_done(),
                                                   total_bar=checkpoint.get_n_total()).run():
        pbar.set_description(f'Hello!')
        if not processed_mapper_result.error and processed_mapper_result.input:
            for idx, inp in enumerate(processed_mapper_result.input):
                checkpoint.add_processed_item(inp)
                reduced = reducer.reduce(item=processed_mapper_result.result[idx], store=reduced)
                checkpoint.set_reduced(reduced)

    # processor.run()
    print(reduced, sum(DUMMY_INPUT))
    assert reduced == sum(DUMMY_INPUT)
    logging.info('Finished execution correctly')
    # logging.info(pformat(reduced))


if __name__ == '__main__':
    main()
