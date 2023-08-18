# -*- coding: utf-8 -*-

import datetime
import copy
import json
import re
from typing import Dict, Any, List, Union

import aword.tools as T


class Segment(dict):
    def __init__(self,
                 body: str,
                 uri: str = None,
                 headings: Union[List[str], str] = '[]',
                 language: str = '',
                 created_by: str = '',
                 last_edited_by: str = '',
                 last_edited_timestamp: Union[datetime.datetime, str] = None,
                 metadata: Union[Dict[str, Any], str] = '{}'):

        super().__init__()

        # Setters will validate and load json
        self.body = body
        self.uri = uri
        self.headings = headings
        self['language'] = language.lower()
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by or created_by
        self.last_edited_timestamp = last_edited_timestamp
        self.metadata = metadata

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name not in ('body',
                        'uri',
                        'headings',
                        'language',
                        'created_by',
                        'last_edited_by',
                        'last_edited_timestamp',
                        'metadata'):
            raise ValueError(f'Invalid Segment field {name}')

        if 'timestamp' in name:
            value = T.timestamp_as_utc(value).isoformat()
        elif name == 'uri':
            value = T.validate_uri(value)
        elif name in ('headings', 'metadata'):
            if isinstance(value, str):
                value = json.loads(value)
        elif name == 'body':
            value = re.sub(r'\n+', '\n', value)
        self[name] = value

    def copy(self):
        return self.__class__(**copy.deepcopy(dict(self)))

    def __str__(self):
        return ' > '.join(self.headings) + '\n\n' + self.body
