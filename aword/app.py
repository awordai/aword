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
    return config_path


class Awd:

    def __init__(self, collection_name=None):
        self.config = {}
        self.json_configs = {}
        self.collection_name = collection_name
        self._embedder = {}
        self._personas = {}
        self._respondents = {}
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

    def get_respondent(self, respondent_name: str):
        if respondent_name not in self._respondents:
            respondents_config = self.get_json_config('respondents')
            if respondent_name not in respondents_config:
                raise RuntimeError(f'No configuration found for respondent {respondent_name}')
            config = respondents_config[respondent_name].copy()

            if 'system_prompt' not in config:
                if 'system_prompt_file' not in config:
                    raise RuntimeError(f'Need a system prompt in the config of {respondent_name}')

                with open(config['system_prompt_file'], encoding='utf-8') as fin:
                    config['system_prompt'] = fin.read()

            if 'user_prompt_preface' not in config and 'user_prompt_preface_file' in config:
                with open(config['user_prompt_preface_file'], encoding='utf-8') as fin:
                    config['user_prompt_preface'] = fin.read()

            from aword.model.respondent import make_respondent

            model_config = self.get_json_config('models')[config['model_name']]
            self._respondents[respondent_name] = make_respondent({**model_config, **config})

        return self._respondents[respondent_name]

    def get_persona(self, persona_name: str):
        if persona_name not in self._personas:
            personas_config = self.get_json_config('personas')
            if persona_name not in personas_config:
                raise RuntimeError(f'No configuration found for persona {persona_name}')
            config = personas_config[persona_name].copy()

            if 'system_prompt' not in config:
                if 'system_prompt_file' not in config:
                    raise RuntimeError(f'Need a system prompt in the config of {persona_name}')

                with open(config['system_prompt_file'], encoding='utf-8') as fin:
                    config['system_prompt'] = fin.read()

            if 'user_prompt_preface' not in config and 'user_prompt_preface_file' in config:
                with open(config['user_prompt_preface_file'], encoding='utf-8') as fin:
                    config['user_prompt_preface'] = fin.read()

            from aword.model.persona import make_persona

            model_config = self.get_json_config('models')[config['model_name']]
            self._personas[persona_name] = make_persona({**model_config, **config})

        return self._personas[persona_name]

    def get_store(self, collection_name: str = None):
        vector_config = self.get_config('vector')
        if collection_name is None:
            collection_name = vector_config.get('collection_name', None)

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

            embedding_config = self.get_config('embedding')
            self._store[collection_name] = make_store(collection_name,
                                                      {**vector_config, **embedding_config})

        return self._store[collection_name]

    def create_store_collection(self, collection_name: str = None):
        store = self.get_store(collection_name)
        store.create_collection(dimensions=self.get_embedder().dimensions)

    def get_source_unit_cache(self):
        if self._source_unit_cache is None:
            cache_config = self.get_config('cache')
            provider = cache_config.get('provider', 'edge')
            processor = import_module(f'aword.cache.{provider}')
            add_summaries = cache_config.get('add_summaries', 'False').lower() == 'true'

            self._source_unit_cache = processor.make_source_unit_cache(
                self.get_persona('summarizer') if add_summaries else None, **cache_config)

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
        source_unit_cache = self.get_source_unit_cache()
        chunk_cache = self.get_chunk_cache(model_name)

        store = self.get_store(collection_name)
        embedder = self.get_embedder(model_name)

        total_chunks = 0
        for source_unit in source_unit_cache.get_unembedded():
            now = datetime.now(utc)
            print('...', source_unit['source_unit_id'])
            chunks = store.store_source_unit(embedder,
                                             source=source_unit['source'],
                                             source_unit_id=source_unit['source_unit_id'],
                                             categories=source_unit['categories'],
                                             scope=source_unit['scope'],
                                             context=source_unit['context'],
                                             language=source_unit['language'],
                                             segments=source_unit['segments'])
            source_unit_cache.flag_as_embedded([source_unit], now=now)
            chunk_cache.add_or_update(source=source_unit['source'],
                                      source_unit_id=source_unit['source_unit_id'],
                                      chunks=chunks)
            print(f'    -> {len(chunks)} chunks')
            total_chunks += len(chunks)

        print(f'Added {total_chunks} chunks')
