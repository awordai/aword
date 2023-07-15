# -*- coding: utf-8 -*-

import datetime
import copy
from typing import Dict, Any, List, Union

import aword.tools as T


class Segment(dict):
    def __init__(self,
                 body: str,
                 uri: str = None,
                 headings: List[str] = None,
                 created_by: str = None,
                 last_edited_by: str = None,
                 last_edited_timestamp: Union[datetime.datetime, str] = None,
                 metadata: Dict[str, Any] = None):

        super().__init__()

        self['body'] = body

        # Setters will validate
        self.uri = uri
        self.last_edited_timestamp = last_edited_timestamp

        self['headings'] = headings or []
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by or created_by
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

    def __str__(self):
        return ' > '.join(self.headings) + '\n\n' + self.body
