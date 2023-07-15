# -*- coding: utf-8 -*-

import os
import json
import configparser
from datetime import datetime
from importlib import import_module
from typing import Dict, List, Union

from pytz import utc


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
        self._summarizer = {}
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

    def get_sources_config(self):
        return self.get_json_config('sources')

    def get_single_source_config(self, source_name: str,
                                 default: Union[Dict, List] = None) -> Union[Dict, List]:
        return self.get_sources_config().get(source_name, default
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
        if model_name is None:
            model_name = embedding_config['model_name']
        else:
            embedding_config['model_name'] = model_name

        if model_name not in self._embedder:
            from aword.model.embedder import make_embedder

            model_config = self.get_json_config('models').get(embedding_config['model_name'], {})
            self._embedder[model_name] = make_embedder({**model_config, **embedding_config})

        return self._embedder[model_name]

    def get_summarizer(self, model_name: str = None):
        summarizing_config = self.get_config('summarizing').copy()
        if model_name is None:
            model_name = summarizing_config['model_name']
        else:
            summarizing_config['model_name'] = model_name

        if model_name not in self._summarizer:
            from aword.model.summarizer import make_summarizer

            model_config = self.get_json_config('models').get(summarizing_config['model_name'], {})
            self._summarizer[model_name] = make_summarizer({**model_config, **summarizing_config})

        return self._summarizer[model_name]

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
            from aword.vector.store import make_store

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
            processor = import_module(f'aword.cache.{provider}')

            self._source_unit_cache = processor.make_source_unit_cache(
                self.get_summarizer(), **cache_config)

        return self._source_unit_cache

    def get_chunk_cache(self, model_name: str = None):
        if model_name is None:
            embedding_config = self.get_config('embedding')
            model_name = embedding_config['model_name']

        if model_name not in self._chunk_cache:
            cache_config = self.get_config('cache').copy()
            provider = cache_config.get('provider', 'edge')
            processor = import_module(f'aword.cache.{provider}')

            cache_config['model_name'] = model_name
            self._chunk_cache[model_name] = processor.make_chunk_cache(**cache_config)

        return self._chunk_cache[model_name]

    def update_cache(self):
        sources = ['local']
        for source_name in sources:
            processor = import_module(f'aword.source.{source_name}')
            processor.add_to_cache(self)

    def embed_and_store(self, collection_name: str = None, model_name: str = None):
        from aword.vector.fields import VectorDbFields

        Source = VectorDbFields.SOURCE.value
        Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
        Categories = VectorDbFields.CATEGORIES.value
        Scope = VectorDbFields.SCOPE.value
        Context = VectorDbFields.CONTEXT.value
        Language = VectorDbFields.LANGUAGE.value

        source_unit_cache = self.get_source_unit_cache()
        chunk_cache = self.get_chunk_cache(model_name)

        store = self.get_store(collection_name)
        embedder = self.get_embedder(model_name)

        for source_unit in source_unit_cache.get_unembedded():

            now = datetime.now(utc)
            chunks = store.store_source_unit(embedder,
                                             source=source_unit[Source],
                                             source_unit_id=source_unit[Source_unit_id],
                                             categories=source_unit[Categories],
                                             scope=source_unit[Scope],
                                             context=source_unit[Context],
                                             language=source_unit[Language],
                                             segments=source_unit['segments'])
            source_unit_cache.flag_as_embedded([source_unit], now=now)
            chunk_cache.add_or_update(source=source_unit[Source],
                                      source_unit_id=source_unit[Source_unit_id],
                                      chunks=chunks)
