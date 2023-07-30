# -*- coding: utf-8 -*-

import uuid
import json
from datetime import datetime
import copy
from typing import List, Dict, Union

import aword.tools as T


def make_id(text):
    return str(uuid.uuid5(uuid.NAMESPACE_X500, text))


class Payload(dict):

    def __init__(self,
                 body: str,
                 source: str = '',
                 source_unit_id: str = '',
                 uri: str = '',
                 categories: Union[Dict, str] = '[]',
                 scope: str = '',  # confidential, public
                 context: str = '',  # historical, reference, internal_comm...
                 language: str = '',
                 headings: Union[List[str], str] = '[]',
                 created_by: str = '',
                 last_edited_by: str = '',
                 last_edited_timestamp: Union[datetime, str] = None,
                 metadata: Union[Dict, str] = '{}'):

        super().__init__()

        self['body'] = body
        self['source'] = source
        self['source_unit_id'] = source_unit_id
        # Setters will validate and load json
        self.uri = uri
        self.categories = categories
        self['scope'] = scope
        self['context'] = context
        self['language'] = language.lower()
        self.headings = headings
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by
        self.last_edited_timestamp = last_edited_timestamp
        self.metadata = metadata

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name not in ('body',
                        'source',
                        'source_unit_id',
                        'uri',
                        'categories',
                        'scope',
                        'context',
                        'language',
                        'headings',
                        'created_by',
                        'last_edited_by',
                        'last_edited_timestamp',
                        'metadata'):
            raise ValueError(f'Invalid Payload field {name}')

        if 'timestamp' in name:
            value = T.timestamp_as_utc(value).isoformat()
        elif name == 'uri':
            value = T.validate_uri(value)
        elif name in ('categories', 'headings', 'metadata'):
            if isinstance(value, str):
                value = json.loads(value)
        self[name] = value

    def copy(self):
        return self.__class__(**copy.deepcopy(dict(self)))

    def __str__(self):
        return ' > '.join(self.headings) + '\n\n' + self.body


class Chunk(dict):

    def __init__(self,
                 payload: Union[Payload, dict],
                 chunk_id: str = None,
                 vector: List[float] = None,
                 vector_db_id: str = None):
        """The payload dictionary should include a source and a
        source_unit_id. Possibly also a category and a scope.
        """
        self.vector = vector
        self.payload = payload if isinstance(payload, Payload) else Payload(**payload)
        self.chunk_id = chunk_id or make_id(payload.body)
        self.vector_db_id = vector_db_id

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name not in ('vector', 'payload', 'chunk_id', 'vector_db_id'):
            raise AttributeError('Cannot have a chunk with attribute ' + name)
        self[name] = value

    def copy(self):
        out = self.__class__(**copy.deepcopy(dict(self)))
        out['payload'] = Payload(**out['payload'])
        return out
