# -*- coding: utf-8 -*-

from typing import List, Dict, Any


class Chunk(dict):
    def __init__(self,
                 text: str,
                 vector: List[float] = None,
                 payload: Dict[str, Any] = None,
                 chunk_id: str = None,
                 vector_db_id: str = None):
        """The payload dictionary should include a source and a
        source_unit_id. Possibly also a category and a scope.
        """
        self.text = text
        self.vector = vector
        self.payload = payload or {}
        self.chunk_id = chunk_id
        self.vector_db_id = vector_db_id

    def __getattr__(self, name):
        if name in self:
            return self[name]
        raise AttributeError(f"No such attribute: {name}")

    def __setattr__(self, name, value):
        if name not in ('text', 'vector', 'payload', 'chunk_id', 'vector_db_id'):
            raise AttributeError('Cannot have a chunk with attribute ' + name)
        self[name] = value

    def copy(self):
        return Chunk(self.text,
                     vector=self.vector,
                     payload=self.payload.copy(),
                     chunk_id=self.chunk_id,
                     vector_db_id=self.vector_db_id)
