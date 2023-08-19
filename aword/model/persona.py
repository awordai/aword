# -*- coding: utf-8 -*-
"""Have conversations with a persona.
"""

from abc import ABC, abstractmethod
import string
from enum import Enum
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
                 chunks_for_last_query,
                 chunks_per_conversation,
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
            ctx_list = []# ['```']

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
                    ctx_list.append(field_label + ': ' + value.replace('\n', ' '))

            # ctx_list.append('```')
            return '\n'.join(ctx_list)

        return [_format_payload(c) for c in payloads]

    def get_background(self,
                       user_query: str,
                       message_history: List[Dict],
                       sources: Union[List[str], str] = None,
                       source_unit_ids: Union[List[str], str] = None,
                       categories: Union[List[str], str] = None,
                       contexts: Union[List[str], str] = None,
                       languages: Union[List[str], str] = None) -> str:
        embedder = self.awd.get_embedder()
        store = self.awd.get_vector_store()

        chunks_for_history = self.chunks_per_conversation - self.chunks_for_last_query
        self.awd.logger.info('Requesting historical background with %d chunks',
                             chunks_for_history)
        historical_background = []
        if message_history:
            historical_background = store.search(
                query_vector=embedder.get_embeddings([' '.join([msg['content']
                                                               for msg in message_history])])[0],
                limit=chunks_for_history,
                sources=sources,
                source_unit_ids=source_unit_ids,
                categories=categories,
                scopes=self.scopes,
                contexts=contexts,
                languages=languages)

        self.awd.logger.info('Requesting current query background with %d chunks',
                             self.chunks_for_last_query)
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
            background_str = '---\n'.join(background)
            return f"\nBackground information:\n{background_str}"

        self.awd.logger.warning('No background available')
        return '\nNo more background available.'

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
                 chunks_per_conversation: int = 5,
                 chunks_for_last_query: int = 4,
                 temperature: int = 1,
                 delimiter: str = '```',
                 background_fields: List[Union[str, tuple]] = None,
                 **params):
        super().__init__(awd=awd,
                         scopes=scopes,
                         chunks_per_conversation=chunks_per_conversation,
                         chunks_for_last_query=chunks_for_last_query,
                         delimiter=delimiter,
                         background_fields=background_fields)
        oai.ensure_api(awd.getenv('OPENAI_API_KEY'))
        self.logger = awd.logger
        self.persona_name = persona_name
        self.model_name = model_name
        self.temperature = temperature
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

    def format_message_history(self, messages):
        background = ''
        out = []
        for message in messages:
            new_background = message.get('background', '')
            if new_background:
                background = new_background
            out.append({'role': message['role'],
                        'content': message['said']})
        return out, background

    def tell(self,
             user_query: str,
             message_history: List[Dict],
             with_background: str = '') -> Dict:

        messages, background = self.format_message_history(message_history)

        if with_background:
            background = with_background

        if self.require_background:
            if not background:
                if not messages:
                    self.logger.info('Getting background for the first message')
                    background = self.get_background(
                        user_query=user_query,
                        message_history=[])

            if not with_background:
                self.logger.info('Requesting to ask for background')
                functions = [self.background_function]
                ask_for_background = ('\n- If the background information is not '
                                      'enough to answer the question request to call '
                                      'the function '
                                      f"{self.background_function['name']}.")
                self.logger.info('Calling completion with %s', self.background_function['name'])
            else:
                functions = []
                ask_for_background = ''
                self.logger.info('Calling completion without background information function')

        self.logger.info('Told @%s (%s): %s',
                         self.persona_name,
                         'no background' if not background else ('%d words background' %
                                                                 len(background.split())),
                         user_query[:80].replace('\n', ' '))

        # The system prompt is not stored with the chat, so it will
        # not come. That enables the same conversation to be shared
        # between several personas
        messages = [{'role': 'system',
                     'content': self.system_prompt + ask_for_background},
                    *messages]

        user_says = user_query
        if not message_history and self.user_prompt_preface:
            user_says = self.user_prompt_preface + '\n' + user_query

        messages.append({'role': 'user',
                         'content': user_says + background})

        out = oai.chat_completion_request(messages=messages,
                                          functions=functions,
                                          model_name=self.model_name,
                                          temperature=self.temperature)

        if out.get('call_function', '') == self.background_function['name']:
            self.logger.info('Model requested background information')
            if message_history:
                summary_text = out.get('with_arguments', {})['summary_text']
                if not summary_text:
                    self.logger.error('Did not receive with_arguments from the model')
                else:
                    summary_text = summary_text + '\n'
            else:
                summary_text = ''

            return self.tell(user_query=user_query,
                             message_history=message_history,
                             with_background=self.get_background(
                                 user_query=summary_text + user_query,
                                 message_history=messages))

        self.logger.info('Replied @%s (%d tokens): %s, %s',
                         self.persona_name,
                         out['total_tokens'],
                         'success' if out['success'] else 'failure',
                         out['reply'][:20])

        out['user_says'] = user_says
        out['background'] = background
        return out
