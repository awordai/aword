# -*- coding: utf-8 -*-
"""Entry point to the Aword set of tools.
"""

import os
import json
import logging
import configparser
from datetime import datetime
from importlib import import_module
from typing import Dict, List, Union

from dotenv import load_dotenv
from pytz import utc

import aword.errors as E


class Awd:

    def __init__(self,
                 environment_name: str,
                 config_dir: str = None,
                 collection_name: str = None):
        self.config_dir = config_dir
        self.environment_name = environment_name

        self.logger = logging.getLogger('aword')
        env_file = '.env.' + self.environment_name
        if os.path.exists(env_file):
            self.logger.info('Loading env file %s', env_file)
            load_dotenv(env_file)
        else:
            self.logger.info('Not found env file %s', env_file)

        self.environment = {
            'TESTING': '',
            'PRODUCTION': '',
            'CONFIG_DIR': '',
            'OPENAI_API_KEY': '',
            'CROWDDEV_TENANT_ID': '',
            'CROWDDEV_API_KEY': '',
            'LINEAR_API_KEY': '',
            'NOTION_API_KEY': '',
            'SLACK_BOT_TOKEN': '',
            'QDRANT_API_KEY': ''
        }
        for env_name, env_val in os.environ.items():
            # Environment variables are as above but with an AWORD_ prefix
            if env_name.startswith('AWORD_'):
                self.environment[env_name[6:]] = env_val

        self.config = {}
        self.json_configs = {}
        self.collection_name = collection_name

        self._embedder = {}
        self._personas = {}
        self._respondents = {}
        self._store = {}
        self._source_unit_cache = None
        self._chunk_cache = {}

    def getenv(self, varname):
        return self.environment.get(varname.upper(), '')

    def is_testing(self) -> bool:
        return self.environment['TESTING'] != ''

    def is_production(self) -> bool:
        return self.environment['PRODUCTION'] != ''

    def find_config(self, fname):
        if self.config_dir is not None:
            full_fname = os.path.join(self.config_dir, fname)
            if os.path.exists(full_fname):
                self.logger.info('Config file %s', full_fname)
                return full_fname

            raise E.AwordError(f'Cannot find config file {full_fname}')

        config_locations = [
            fname,
            os.path.join(self.environment['CONFIG_DIR'], fname),
            os.path.join(os.path.expanduser('~/.aword'), fname),
            # TODO: this should be packaged and shipped.
            os.path.join(os.path.dirname(__file__), '../res', fname)  # repository
        ]

        config_path = next((path for path in config_locations
                            if path and os.path.exists(path)), None)

        if config_path is None:
            raise E.AwordError('Cannot find a config file')

        self.logger.info('Found config file %s', config_path)

        return config_path

    def get_json_config(self, config_name: str) -> Dict:
        if not config_name in self.json_configs:
            config_path = self.find_config(config_name + '.json')

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

            config_path = self.find_config('config.ini')

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
            self._embedder[model_name] = make_embedder(self, {**model_config, **embedding_config})

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
            self._respondents[respondent_name] = make_respondent(self, {**model_config, **config})

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
            self._personas[persona_name] = make_persona(self, {**model_config, **config})

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
            self._store[collection_name] = make_store(self,
                                                      collection_name,
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
        sources = ['local', 'notion']
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
            self.logger.info('Embedding and storing source unit (%s, %s)',
                             str(source_unit['source']),
                             str(source_unit['source_unit_id']))
            chunks = store.store_source_unit(embedder,
                                             source=source_unit['source'],
                                             source_unit_id=source_unit['source_unit_id'],
                                             categories=source_unit['categories'],
                                             scope=source_unit['scope'],
                                             context=source_unit['context'],
                                             language=source_unit['language'],
                                             segments=source_unit['segments'])
            source_unit_cache.flag_as_embedded([source_unit], now=now)

            # First remove the chunks for this source unit. Otherwise
            # old chunks will be left if this is an edit.
            chunk_cache.delete_source_unit(source=source_unit['source'],
                                           source_unit_id=source_unit['source_unit_id'])
            chunk_cache.add(source=source_unit['source'],
                            source_unit_id=source_unit['source_unit_id'],
                            chunks=chunks)
            total_chunks += len(chunks)

        self.logger.info('Added %d chunks', total_chunks)


def add_args(parser):
    parser.add_argument('--embed-cache',
                        help=('Chunks and embeds the unembedded segments in the cache, and '
                              'stores them in the vector database'),
                        action='store_true')


def main(awd, args):
    if args['embed_cache']:
        awd.embed_and_store()


def app():
    import argparse

    core_arguments = {
        ('-v', '--verbose'): {'help': 'Enable info logs.',
                              'action': 'store_true'},
        ('-d', '--debug'): {'help': 'Enable debug logs.',
                            'action': 'store_true'},
        ('-e', '--environment'): {'help': ('Environment name. If set, for example, to test '
                                           'the file .env.test will be loaded if existing.'),
                                  'type': str,
                                  'default': 'dev'},
        ('-L', '--error-logs-dir'): {'help': 'Directory for the error logs.',
                                     'type': str,
                                     'default': 'logs'},
        ('-C', '--config-dir'): {'help': ('Directory with the configuration files. If present '
                                          'it will not attempt to search for them in the '
                                          'default places (the value of the AWORD_CONFIG_DIR '
                                          'environment variable and the ~/.aword folder)'),
                                 'type': str}
    }

    parser_config = argparse.ArgumentParser(add_help=False)

    for argument, options in core_arguments.items():
        parser_config.add_argument(*argument, **options)

    global_args, cmdline = parser_config.parse_known_args()

    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.ArgumentDefaultsHelpFormatter,
                                     allow_abbrev=True)

    # These three are here only for the help.  They are captured by the previous
    # command-line parsing (required by the config) so will never reach this point.
    # The previous one cannot have help, otherwise the help all the coming arguments
    # in the commands would never be shown.
    for argument, options in core_arguments.items():
        parser.add_argument(*argument, **options)


    from aword.logger import configure_logging
    configure_logging(debug=global_args.debug,
                      silent=not global_args.verbose,
                      error_logs_dir=global_args.error_logs_dir)

    # These imports happen after the logging has been
    # configured. Otherwise they could get an unconfigured logger.
    import aword.source.notion
    import aword.source.local
    import aword.model.persona
    import aword.model.respondent
    import aword.cache.cache
    commands = {
        'notion': aword.source.notion,
        'local': aword.source.local,
        'chat': aword.model.persona,
        'ask': aword.model.respondent,
        'cache': aword.cache.cache
    }

    subparsers = parser.add_subparsers(title='Commands')
    subparser = subparsers.add_parser('app',
                                      help='Main application.',
                                      formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    add_args(subparser)
    subparser.set_defaults(func=main)

    for command, module in commands.items():
        subparser = subparsers.add_parser(command,
                                          help=module.__doc__,
                                          formatter_class=argparse.ArgumentDefaultsHelpFormatter)
        module.add_args(subparser)
        subparser.set_defaults(func=module.main)

    command = cmdline[0]
    cmdline_args = cmdline[1:]

    args, unknown = parser.parse_known_args([command] + cmdline_args)
    if unknown:
        raise RuntimeError('error: unrecognized arguments: ' + str(unknown))

    command_function = args.func

    # We want to deal with a dictionary, not a Namespace object.
    dict_args = vars(args)

    # Remove the arguments that we don't want used by the command function
    dict_args.pop('func')
    for argument in core_arguments.keys():
        dict_args.pop(argument[1].replace('--', '').replace('-', '_'))
    dict_args['mode'] = command

    awd = Awd(environment_name=global_args.environment,
              config_dir=global_args.config_dir)
    command_function(awd, dict_args)


if __name__ == '__main__':
    app()
