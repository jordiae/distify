from sqlitedict import SqliteDict
import os
from typing import Optional
from ordered_set import OrderedSet
import time
import uuid
from hydra.core.config_store import ConfigStore
from dataclasses import dataclass


class CheckpointSet:
    def __init__(self, path, inputs: Optional[OrderedSet]):
        self.data = SqliteDict(os.path.join(path, 'checkpoint.db'), autocommit=True, tablename='checkpoint')
        if 'done' not in self.data:
            self.data['done'] = OrderedSet([])
            assert inputs is not None
            self.data['all_items'] = inputs
        else:
            assert inputs is None
            assert 'all_items' in self.data
        if 'reduced' not in self.data:
            self.data['reduced'] = None

    @classmethod
    def load(cls, path):
        return cls(path, inputs=None)

    @property
    def inputs(self):
        return self.data['all_items']

    @property
    def inputs_done(self):
        return self.data['done']

    @staticmethod
    def exists(path):
        return os.path.exists(os.path.join(path, 'checkpoint.db'))

    def mark_as_done(self, item):
        done = self.data['done']
        done.add(item)
        self.data['done'] = done

    def set_reduced(self, result):
        self.data['reduced'] = result

    def get_reduced(self):
        return self.data['reduced']


class CheckpointMask:
    def __init__(self, path, inputs: Optional[OrderedSet], blocking=True):
        self.data = SqliteDict(os.path.join(path, 'checkpoint.db'), tablename='checkpoint')
        if inputs:
            for inp in inputs:
                self.data[hash(inp)] = (False, inp)  # Not done. Note that e.g. ints, in keys, are stored as strings...
            self.data['__reduced__'] = None
            self.data.commit()

        self.blocking = blocking

    @classmethod
    def load(cls, path):
        return cls(path, inputs=None)

    @property
    def inputs_to_do(self):
        return OrderedSet([self.data[item][1] for item in self.data if item != '__reduced__' and not self.data[item][0]])

    @property
    def inputs(self):
        return OrderedSet(
            [self.data[item][1] for item in self.data if item != '__reduced__'])

    @property
    def inputs_done(self):
        return OrderedSet([self.data[item][1] for item in self.data if item != '__reduced__' and self.data[item][0]])

    @staticmethod
    def exists(path):
        return os.path.exists(os.path.join(path, 'checkpoint.db'))

    def mark_as_done(self, item):
        self.data[hash(item)] = (True, item)

    def set_reduced(self, result):
        self.data['__reduced__'] = result

    def get_reduced(self):
        return self.data['__reduced__']

    def commit(self):
        self.data.commit(blocking=self.blocking)

@dataclass
class DistifyConfig: pass

def register_configs():
    cs = ConfigStore.instance()
    cs.store(
        name="config",
        node=DistifyConfig,
    )


register_configs()

def get_process_unique_path():
    def timestamp():
        return time.strftime("%Y-%m-%d-%H%M")
    process_id = os.uname()[1] + '_' + str(os.getpid())
    ts = timestamp()
    extra_id = uuid.uuid4().hex
    return os.path.join(os.getcwd(), process_id + '_' + ts + '_' + extra_id)