# -*- coding: utf-8 -*-

import os
import json
import configparser
from typing import Dict, List, Union

from aword.embedding.model import make_embedder
from aword.vector.store import make_store
from aword.cache import edge


def find_config(fname):
    config_locations = [
        fname,
        os.path.join(os.environ.get('AWORD_CONFIG', ''), fname),
        os.path.join(os.path.expanduser('~/.aword'), fname),
        # TODO: this should be packaged and shipped.  Maybe move to poetry
        os.path.join(os.path.dirname(__file__), '../res', fname)  # repository
    ]

    config_path = next((path for path in config_locations
                        if path and os.path.isfile(path)), None)
    print(f'Found {config_path}')
    return config_path


class Awd:

    def __init__(self, collection_name=None):
        self.config = {}
        self.json_configs = {}
        self.collection_name = collection_name
        self._embedder = {}
        self._store = {}
        self._source_unit_cache = None
        self._chunk_cache = {}

    def get_json_config(self, config_name: str) -> Dict:
        if not config_name in self.json_configs:
            config_path = find_config(config_name + '.json')

            if config_path is None:
                self.json_configs[config_name] = {}

            with open(config_path, 'r', encoding='utf-8') as f:
                self.json_configs[config_name] = json.load(f)

        return self.json_configs[config_name]

    def get_source_config(self, source_name: str,
                          default: Union[Dict, List] = None) -> Union[Dict, List]:
        return self.get_json_config('sources').get(source_name, default
                                                   if default is not None else {})

    def get_model_config(self, model_name: str = None) -> Dict:
        embedding_config = self.get_config('embedding')

        return self.get_json_config('models').get(model_name or embedding_config['model_name'],
                                                  {})

    def get_config(self, section: str) -> Dict:
        if not self.config:
            config = configparser.ConfigParser()

            config_path = find_config('config.ini')

            if config_path is None:
                raise FileNotFoundError('No configuration file found.')

            config.read(config_path)

            for s in config.sections():
                self.config[s] = dict(config[s])

                for key, value in self.config[s].items():
                    try:
                        if '.' in value:
                            self.config[s][key] = float(value)
                        else:
                            self.config[s][key] = int(value)
                    except ValueError:
                        pass

        return self.config.get(section, {})

    def get_embedder(self, model_name: str = None):
        embedding_config = self.get_config('embedding').copy()
        if model_name is not None:
            embedding_config['model_name'] = model_name

        if model_name not in self._embedder:
            model_config = self.get_json_config('models').get(embedding_config['model_name'], {})
            self._embedder[model_name] = make_embedder({**model_config, **embedding_config})

        return self._embedder[model_name]

    def get_store(self, collection_name: str = None):
        if collection_name is None:
            # If there's only one store, return it
            if len(self._store) == 1:
                return next(iter(self._store.values()))
            if not self._store:
                raise ValueError('There are no stores available, please specify a collection name')
            raise ValueError("There are multiple stores available, "
                             "please specify a collection name.")

        if collection_name not in self._store:
            vector_config = self.get_config('vector')
            embedding_config = self.get_config('embedding')
            self._store[collection_name] = make_store(collection_name,
                                                      {**vector_config, **embedding_config})

        return self._store[collection_name]

    def create_store_collection(self, collection_name: str):
        store = self.get_store(collection_name)
        store.create_collection(dimensions=self.get_embedder().dimensions)

    def get_source_unit_cache(self):
        if self._source_unit_cache is None:
            cache_config = self.get_config('cache')
            provider = cache_config.get('provider', 'edge')

            if provider == 'edge':
                self._source_unit_cache = edge.make_source_unit_cache(**cache_config)
            else:
                raise RuntimeError(f'Unknown cache provider {provider}')

        return self._source_unit_cache


    def get_chunk_cache(self, model_name: str = None):
        if model_name is None:
            embedding_config = self.get_config('embedding')
            model_name = embedding_config['model_name']

        if model_name not in self._chunk_cache:
            cache_config = self.get_config('cache')
            provider = cache_config.get('provider', 'edge')

            if provider == 'edge':
                self._chunk_cache[model_name] = edge.make_source_unit_cache(**cache_config)
            else:
                raise RuntimeError(f'Unknown cache provider {provider}')

        return self._chunk_cache[model_name]
