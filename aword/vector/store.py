# -*- coding: utf-8 -*-

import os
import uuid
from typing import List, Dict
from abc import ABC, abstractmethod

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct


import aword.tools as T
from aword.embedding.model import Embedder
from aword.chunk import Chunk
from aword.segment import Segment
from aword.vector.fields import VectorDbFields


Source = VectorDbFields.SOURCE.value
Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
Category = VectorDbFields.CATEGORY.value
Scope = VectorDbFields.SCOPE.value


def make_store(collection_name: str,
               config: Dict):
    provider = config.get('provider', 'qdrant')
    if provider == 'qdrant':
        return QdrantStore(collection_name, **config)

    raise ValueError(f'Unknown vector store provider {provider}')


def make_id(source_unit_id, text):
    return str(uuid.uuid5(uuid.NAMESPACE_X500, source_unit_id + text))


class Store(ABC):

    @abstractmethod
    def clean_source_unit(self,
                          source_unit_id: str):
        """We assume that when a source unit is embedded it will replace
        any previous version of the same unit, so we first need to
        delete all the points belonging to this source_unit_id if any
        exists.
        """

    @abstractmethod
    def upsert_chunks(self,
                      chunks: List[Chunk]) -> List[Chunk]:
        """Upsert a list of extended chunks to the vector
        database. Extended chunks are dictionaries based in Chunks,
        with an id, a source_unit_id, a source, a category and a
        scope.
        """

    def store_source_unit(self,
                          embedder: Embedder,
                          source: str,
                          source_unit_id: str,
                          category: str,
                          scope: str,
                          segments: List[Segment]) -> List[Chunk]:

        to_upsert = []
        for segment in segments:
            # TODO Maybe it should include the headings in the
            # embedding.  Maybe do the embedding separately, and then
            # average it with a weight. It will probably not work with
            # non normalized embeddings.
            chunks = embedder.get_embedded_chunks(segment.body)
            for chunk in chunks:
                chunk.payload[Source] = source
                chunk.payload[Source_unit_id] = source_unit_id
                chunk.payload[Category] = category
                chunk.payload[Scope] = scope
                chunk.payload['headings'] = segment.headings
                chunk.payload['created_by'] = segment.created_by
                chunk.payload['last_edited_by'] = segment.last_edited_by
                chunk.payload['last_edited_timestamp'] = segment.last_edited_timestamp
                chunk.payload['metadata'] = segment.metadata

                to_upsert.append(chunk)

        self.clean_source_unit(source_unit_id)
        return self.upsert_chunks(to_upsert)


class QdrantStore(Store):

    def __init__(self,
                 collection_name,
                 local_db: str = None,
                 url: str = None,
                 distance: str = 'cosine',
                 **_):
        self.collection_name = collection_name
        self.distance = distance

        if url:
            T.load_environment()
            client_pars = {'url': url,
                           'api_key': os.environ['QDRANT_API_KEY']}
        elif local_db:
            client_pars = {'path': local_db}
        else:
            raise RuntimeError('Need either local_db or url')

        self.client = QdrantClient(**client_pars)

    def create_collection(self, dimensions: int):
        distance = {'dot': Distance.DOT,
                    'cosine': Distance.COSINE,
                    'euclid': Distance.EUCLID}[self.distance]
        self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=VectorParams(size=dimensions,
                                        distance=distance))

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SOURCE_UNIT_ID.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SOURCE.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.CATEGORY.value,
                                         field_schema="keyword")

        # TODO there can be several scopes (it's actually an array),
        # check that the schema is keyword.
        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SCOPE.value,
                                         field_schema="keyword")

    def count(self) -> int:
        return self.client.count(collection_name=self.collection_name,
                                 exact=True).count

    def search(self,
               query_vector: List[float],
               limit: int) -> List[Dict]:
        out = self.client.search(collection_name=self.collection_name,
                                 query_vector=query_vector,
                                 limit=limit)
        return [r.payload for r in out]

    def clean_source_unit(self, source_unit_id: str):
        self.client.delete(collection_name=self.collection_name,
                           points_selector=Filter(must=[
                               FieldCondition(key=Source_unit_id,
                                              match=MatchValue(value=source_unit_id))]))

    def upsert_chunks(self, chunks: List[Chunk]) -> List[Chunk]:
        out = []
        points = []
        for chunk in chunks:
            payload = chunk.payload.copy()
            vector_db_id = make_id(payload.get(Source, '') + payload.get(Source_unit_id, ''),
                                   chunk.text)
            points.append(PointStruct(id=vector_db_id,
                                      vector=chunk.vector,
                                      payload=payload))

            out_chunk = chunk.copy()
            out_chunk.payload = payload  # The copy
            out_chunk.vector_db_id = vector_db_id
            out.append(out_chunk)

        self.client.upsert(collection_name=self.collection_name,
                           points=points)
        return out
