# -*- coding: utf-8 -*-

import datetime
from typing import Optional, Dict, Any, List, Union


class Segment(dict):
    def __init__(self,
                 body: str,
                 uri: str = None,
                 headings: List[str] = None,
                 created_by: str = None,
                 last_edited_by: str = None,
                 last_edited_timestamp: Union[datetime.datetime, str] = None,
                 last_embedded_timestamp: Union[datetime.datetime, str] = None,
                 metadata: Dict[str, Any] = None):

        super().__init__()

        self['body'] = body

        # Setters will validate
        self.uri = uri
        self.last_edited_timestamp = last_edited_timestamp
        self.last_embedded_timestamp = last_embedded_timestamp

        self['headings'] = headings or []
        self['created_by'] = created_by
        self['last_edited_by'] = last_edited_by or created_by
        self['metadata'] = metadata or {}


        for key, value in vector_db_fields.items():
            if key not in VectorDbFields.__members__:
                raise ValueError(f"Invalid key: {key}. "
                                 f"Must be one of {list(VectorDbFields.__members__.keys())}")
            self[key] = value

        for field in VectorDbFields:
            if not field.value in self:
                self[field.value] = ''

        if not self[VectorDbFields.SOURCE.value]:
            raise ValueError('A Segment needs a source')

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
