from sqlitedict import SqliteDict
import os
from typing import Set, Optional
from ordered_set import OrderedSet
import signal


class Checkpoint:
    def __init__(self, path, all_items: Optional[OrderedSet] = None):
        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

        self.data = SqliteDict(os.path.join(path, 'checkpoint.db'), autocommit=True)
        if 'done' not in self.data:
            self.data['done'] = OrderedSet([])
            assert all_items is not None
            self.data['all_items'] = all_items
        else:
            assert all_items is None
            assert 'all_items' in self.data
        if 'reduced' not in self.data:
            self.data['reduced'] = None

    def exit_gracefully(self, *args):
        self.data.close()

    @staticmethod
    def exists(path):
        return os.path.exists(os.path.join(path, 'checkpoint.db'))

    def add_processed_item(self, item):
        done = self.data['done']
        done.add(item)
        self.data['done'] = done

    def set_reduced(self, result):
        self.data['reduced'] = result

    def get_reduced(self):
        return self.data['reduced']

    def get_not_done_items(self):
        return self.data['done'].intersection(self.data['all_items'])

    def get_n_done(self):
        return len(self.data['done'])

    def get_n_total(self):
        return len(self.data['all_items'])


class Reducer:
    def reduce(self, item, store):
        raise NotImplementedError

    @property
    def default_value(self):
        raise NotImplementedError
