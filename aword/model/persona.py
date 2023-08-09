# -*- coding: utf-8 -*-
"""Have conversations with a persona.
"""

import string
from enum import Enum
from typing import Dict, List, Union

import aword.tools as T
from aword.apis import oai
from aword.chunk import Payload
from aword.model.embedder import Embedder
from aword.vector.store import Store


def make_persona(awd,
                 persona_name: str,
                 config: Dict):
    provider = config.get('provider', 'openai')

    if provider == 'openai':
        return OAIPersona(awd, persona_name, **config)

    raise ValueError(f"Unknown model provider '{provider}'")


class Persona:

    def __init__(self,
                 scopes: List[str],
                 chunks_per_conversation: int = 20,
                 delimiter: str = '```',
                 background_fields: List[Union[str, tuple]] = None):
        self.scopes = scopes
        self.chunks_per_conversation = chunks_per_conversation
        self.delimiter = delimiter
        self.background_fields = background_fields or (
            'source',
            ('created_by', 'Created by'),
            ('last_edited_by', 'Last edited by'),
            ('last_edited_timestamp', 'Last edited timestamp'),
            'categories',
            'context',  # historical, reference, internal_comm...
            ('headings', 'Breadcrumbs', ' > '),
            ('uri', 'URL or file'),
            'body')

    def format_background(self, payloads: List[Payload]) -> List[str]:

        def _format_payload(payload: Payload) -> str:
            ctx_list = ['```']

            def _str(_value, _name):
                if _name == 'uri':
                    return T.uri_to_file_path(_value) if _value.startswith('file:') else _value
                if isinstance(_value, Enum):
                    return _value.value
                return str(value)

            for field in self.background_fields:
                if isinstance(field, tuple):
                    field_name, field_label = field[:2]
                    if len(field) == 3:
                        separator = field[2]
                else:
                    field_name = field
                    field_label = field.title()
                    separator = ', '

                value = payload[field_name]

                if isinstance(value, (list, tuple)):
                    value = separator.join([_str(v, field_name) for v in value])
                else:
                    value = _str(value, field_name)

                if value:
                    ctx_list.append(field_label + ': ' + value)

            ctx_list.append('```')
            return '\n'.join(ctx_list)

        return [_format_payload(c) for c in payloads]

    def get_background(self,
                       starter_question: str,
                       embedder: Embedder,
                       store: Store,
                       sources: Union[List[str], str] = None,
                       source_unit_ids: Union[List[str], str] = None,
                       categories: Union[List[str], str] = None,
                       contexts: Union[List[str], str] = None,
                       languages: Union[List[str], str] = None):
        return store.search(query_vector=embedder.get_embeddings([starter_question]),
                            limit=self.chunks_per_conversation,
                            sources=sources,
                            source_unit_ids=source_unit_ids,
                            categories=categories,
                            scopes=self.scopes,
                            contexts=contexts,
                            languages=languages)


class OAIPersona(Persona):

    def __init__(self,
                 awd,
                 persona_name,
                 scopes: List[str],
                 model_name: str,
                 system_prompt: str,
                 user_prompt_preface: str = '',
                 chunks_per_conversation: int = 20,
                 delimiter: str = '```',
                 background_fields: List[Union[str, tuple]] = None,
                 **params):
        super().__init__(scopes=scopes,
                         chunks_per_conversation=chunks_per_conversation,
                         delimiter=delimiter,
                         background_fields=background_fields)
        oai.ensure_api(awd.getenv('OPENAI_API_KEY'))
        self.logger = awd.logger
        self.persona_name = persona_name
        self.model_name = model_name
        self.params = params
        self.system_prompt = string.Template(system_prompt).substitute(params)
        self.user_prompt_preface = string.Template(user_prompt_preface).substitute(params)

    def get_param(self, parname: str):
        return self.params[parname]

    def chat(self, text: str):
        self.logger.info('ask @%s: %s', self.persona_name, text[:60])
        return oai.chat(model_name=self.model_name,
                        system_prompt=self.system_prompt,
                        user_prompt=self.user_prompt_preface + text)


def add_args(parser):
    import argparse
    parser.add_argument('persona', help='Persona')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    persona_name = args['persona'].replace('@', '')
    question = ' '.join(args['question'])
    persona = awd.get_persona(persona_name)
    print(persona.chat(question))
