# -*- coding: utf-8 -*-

import datetime
import copy

from typing import Any, Dict, List, Union

import aword.tools as T


class Chunk(dict):
    def __init__(self,
                 text: str,
                 vector: List[float] = None,
                 vector_db_id: str = ''):
        self.text = text
        self.vector = vector
        self.vector_db_id = vector_db_id

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name not in ('text', 'vector', 'vector_db_id'):
            raise AttributeError('Cannot have a chunk with attribute ' + name)
        self[name] = value


class Segment(dict):
    def __init__(self,
                 body: str,
                 source: str = '',
                 source_unit_id: str = None,
                 category: str = '',
                 scope: str = '',
                 uri: str = None,
                 headings: List[str] = None,
                 created_by: str = None,
                 last_edited_by: str = None,
                 last_edited_timestamp: Union[datetime.datetime, str] = None,
                 last_embedded_timestamp: Union[datetime.datetime, str] = None,
                 chunks: List[Chunk] = None,
                 metadata: Dict[str, Any] = None):
        super().__init__()

        self['body'] = body
        self['source_unit_id'] = source_unit_id
        self['source'] = source
        self['category'] = category
        self['scope'] = scope

        # Setters will validate
        self.uri = uri
        self.last_edited_timestamp = last_edited_timestamp
        self.last_embedded_timestamp = last_embedded_timestamp

        self['headings'] = headings or []
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by or created_by
        self.chunks = chunks
        self['metadata'] = metadata or {}

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if 'timestamp' in name:
            value = T.timestamp_as_utc(value).isoformat()
        elif name == 'uri':
            value = T.validate_uri(value)
        elif name == 'headings':
            if isinstance(value, tuple):
                value = list(value)
            elif not isinstance(value, list):
                value = [value]

        self[name] = value

    @classmethod
    def copy(cls, instance):
        return cls(**copy.deepcopy(dict(instance)))
