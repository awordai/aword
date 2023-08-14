# -*- coding: utf-8 -*-
"""Have conversations with a persona.
"""

from abc import ABC, abstractmethod
import string
from enum import Enum
from pprint import pformat
from typing import Dict, List, Union

import aword.tools as T
from aword.apis import oai
from aword.chunk import Payload


def make_persona(awd,
                 persona_name: str,
                 config: Dict):
    provider = config.get('provider', 'openai')

    if provider == 'openai':
        return OAIPersona(awd, persona_name, **config)

    raise ValueError(f"Unknown model provider '{provider}'")


class Persona(ABC):

    def __init__(self,
                 awd,
                 scopes: List[str],
                 chunks_for_last_query: int = 10,
                 chunks_per_conversation: int = 20,
                 delimiter: str = '```',
                 background_fields: List[Union[str, tuple]] = None,
                 require_background=True):
        """The chunks per conversation are divided in two groups:
        chunks_for_last_query will be semantically similar the last
        query only, and the rest up to chunks_per_conversation will be
        similar to the concatenation of user questions in the
        conversation history.
        """
        self.awd = awd
        self.scopes = scopes
        self.chunks_for_last_query = chunks_for_last_query
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
        self.require_background = require_background

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
                       user_query: str,
                       message_history: List[Dict],
                       collection_name: str = None,
                       sources: Union[List[str], str] = None,
                       source_unit_ids: Union[List[str], str] = None,
                       categories: Union[List[str], str] = None,
                       contexts: Union[List[str], str] = None,
                       languages: Union[List[str], str] = None) -> str:
        embedder = self.awd.get_embedder()
        store = self.awd.get_store(collection_name=collection_name)

        historical_background = []
        if message_history:
            historical_background = store.search(
                query_vector=embedder.get_embeddings([msg['content']
                                                      for msg in message_history])[0],
                limit=self.chunks_per_conversation - self.chunks_for_last_query,
                sources=sources,
                source_unit_ids=source_unit_ids,
                categories=categories,
                scopes=self.scopes,
                contexts=contexts,
                languages=languages)

        user_query_background = store.search(
            query_vector=embedder.get_embeddings([user_query])[0],
            limit=self.chunks_for_last_query,
            sources=sources,
            source_unit_ids=source_unit_ids,
            categories=categories,
            scopes=self.scopes,
            contexts=contexts,
            languages=languages)

        background = (self.format_background(historical_background) +
                      self.format_background(user_query_background))
        if background:
            background_str = '\n'.join(background)
            return f"\n\nBackground information:\n\n{background_str}\n\n"

        self.awd.logger.warning('No background available')
        return '\n\nNo more background available.\n\n'

    @abstractmethod
    def tell(self,
             user_query: str,
             message_history: List[Dict],
             **_) -> Dict:
        """Receive a user query, and offer a reply.
        """


class OAIPersona(Persona):

    def __init__(self,
                 awd,
                 persona_name: str,
                 scopes: List[str],
                 model_name: str,
                 system_prompt: str,
                 user_prompt_preface: str = '',
                 chunks_per_conversation: int = 20,
                 delimiter: str = '```',
                 background_fields: List[Union[str, tuple]] = None,
                 **params):
        super().__init__(awd=awd,
                         scopes=scopes,
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
        self.background_function = {
            "name": "get_background_information",
            "description": "Get relevant background information to generate a correct answer.",
            "parameters": {
                "type": "object",
                "properties": {
                    "summary_text": {
                        "type": "string",
                        "description": ("A description of what the required "
                                        "background information is for.")
                    }
                },
                "required": ["summary_text"]
            }
        }

    def get_param(self, parname: str):
        return self.params[parname]

    def tell(self,
             user_query: str,
             message_history: List[Dict],
             collection_name: str = None,
             temperature: float = 1,
             background: str = '') -> Dict:

        self.logger.info('Told @%s (%s): %s',
                         self.persona_name,
                         'no background' if not background else ('%d words background' %
                                                                 len(background.split())),
                         user_query[:80].replace('\n', ' '))

        if self.require_background and not background:
            self.logger.info('Requesting to ask for background')
            functions = [self.background_function]
            ask_for_background = ('\n- If you do not have enough background information, '
                                  'or if the background information does not appear '
                                  f"to be relevant, call the {self.background_function['name']} "
                                  'function.')
            self.logger.info('Calling completion with %s', self.background_function['name'])
        else:
            functions = []
            ask_for_background = ''
            self.logger.info('Calling completion without background information function')

        # The system prompt is not stored with the chat, so it will
        # not come. That enables the same conversation to be shared
        # between several personas
        messages = [{'role': 'system',
                     'content': self.system_prompt + ask_for_background},
                    *message_history]

        user_says = user_query
        if not message_history and self.user_prompt_preface:
            user_says = self.user_prompt_preface + '\n\n' + user_query

        messages.append({'role': 'user',
                         'content': background + user_says})

        out = oai.chat_completion_request(messages=messages,
                                          functions=functions,
                                          model_name=self.model_name,
                                          temperature=temperature)

        if out.get('call_function', '') == self.background_function['name']:
            self.logger.info('Model requested background information')
            if message_history:
                summary_text = out.get('with_arguments', '')
                if not summary_text:
                    self.logger.error('Did not receive with_arguments from the model')
                else:
                    summary_text = summary_text + '\n\n'
            else:
                summary_text = ''

            return self.tell(user_query=user_query,
                             message_history=message_history,
                             collection_name=collection_name,
                             temperature=temperature,
                             background=self.get_background(
                                 user_query=summary_text + user_query,
                                 message_history=message_history,
                                 collection_name=collection_name))

        self.logger.info('Replied @%s: %s, %s',
                         self.persona_name,
                         'success' if out['success'] else 'failure',
                         out['reply'][:80])

        out['user_says'] = user_says
        out['background'] = background
        return out
