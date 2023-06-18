# -*- coding: utf-8 -*-

import datetime
from enum import Enum

from typing import Any, Dict, List, Union

import aword.tools as T


class FactType(str, Enum):
    reference = 'reference'
    historical = 'historical'


class Payload(dict):
    def __init__(self,
                 body: str,
                 source_unit_id: str = None,
                 uri: str = None,
                 headings: List[str] = None,
                 created_by: str = None,
                 last_edited_by: str = None,
                 source: str = '',
                 fact_type: FactType = FactType.historical,
                 timestamp: Union[datetime.datetime, str] = None,
                 metadata: Dict[str, Any] = None):
        super().__init__()

        self['body'] = body
        self['source_unit_id'] = source_unit_id

        # Setter will validate
        self.uri = uri
        self.timestamp = timestamp

        self['headings'] = headings or []
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by or created_by
        self['source'] = source
        self['fact_type'] = fact_type
        self['metadata'] = metadata or {}

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name == 'timestamp':
            value = T.timestamp_as_utc(value).isoformat()
        elif name == 'uri':
            value = T.validate_uri(value)
        elif name == 'headings':
            if isinstance(value, tuple):
                value = list(value)
            elif not isinstance(value, list):
                value = [value]

        self[name] = value
