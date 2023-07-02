# -*- coding: utf-8 -*-

from typing import List


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
