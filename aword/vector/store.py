# -*- coding: utf-8 -*-

import os
import uuid
from typing import List, Dict, Union
from abc import ABC, abstractmethod

from qdrant_client import models
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct


import aword.tools as T
from aword.embedding.model import Embedder
from aword.chunk import Chunk
from aword.segment import Segment
from aword.vector.fields import VectorDbFields


Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
Source = VectorDbFields.SOURCE.value
Categories = VectorDbFields.CATEGORIES.value
Scope = VectorDbFields.SCOPE.value
Context = VectorDbFields.CONTEXT.value


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
        with an id, a source_unit_id, a source, categories, a
        scope and a context.
        """

    def store_source_unit(self,
                          embedder: Embedder,
                          source: str,
                          source_unit_id: str,
                          categories: str,
                          scope: str,
                          context: str,
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
                chunk.payload[Categories] = categories
                chunk.payload[Scope] = scope
                chunk.payload[Context] = context
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
        distance = {'dot': models.Distance.DOT,
                    'cosine': models.Distance.COSINE,
                    'euclid': models.Distance.EUCLID}[self.distance]
        self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(size=dimensions,
                                               distance=distance))

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SOURCE_UNIT_ID.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SOURCE.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.CATEGORIES.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.SCOPE.value,
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name=VectorDbFields.CONTEXT.value,
                                         field_schema="keyword")

    def count(self) -> int:
        return self.client.count(collection_name=self.collection_name,
                                 exact=True).count

    def create_filter(self,
                      sources: Union[List[str], str] = None,
                      categories: Union[List[str], str] = None,
                      scopes: Union[List[str], str] = None,
                      contexts: Union[List[str], str] = None) -> models.Filter:
        """There are three filter possibilities: source, categories,
        scope and context. If more than one value comes for each, any
        of the values should match. If more than one filter comes all
        should be applied.
        """
        where = {}
        if sources:
            where[Source] = sources
        if categories:
            where[Categories] = categories
        if scopes:
            where[Scope] = scopes
        if contexts:
            where[Context] = contexts

        conditions = []
        for key, value in where.items():
            if value:
                if isinstance(value, list):
                    condition = models.FieldCondition(
                        key=key,
                        match=models.MatchAny(any=value),
                    )
                else:
                    condition = models.FieldCondition(
                        key=key,
                        match=models.MatchValue(value=value),
                    )
                conditions.append(condition)
        return models.Filter(must=conditions)

    def search(self,
               query_vector: List[float],
               limit: int,
               sources: Union[List[str], str] = None,
               categories: Union[List[str], str] = None,
               scopes: Union[List[str], str] = None,
               contexts: Union[List[str], str] = None) -> List[Dict]:

        out = self.client.search(collection_name=self.collection_name,
                                 query_vector=query_vector,
                                 query_filter=self.create_filter(sources=sources,
                                                                 categories=categories,
                                                                 scopes=scopes,
                                                                 contexts=contexts),
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

    def retrieve(self,
                 chunk_ids: List[str],
                 with_payload: bool = True,
                 with_vectors: bool = False) -> List[Dict]:

        return self.client.retrieve(collection_name=self.collection_name,
                                    ids=chunk_ids,
                                    with_payload=with_payload,
                                    with_vectors=with_vectors)

    def fetch_all(self,
                  sources: Union[List[str], str] = None,
                  categories: Union[List[str], str] = None,
                  scopes: Union[List[str], str] = None,
                  contexts: Union[List[str], str] = None,
                  with_payload: bool = True,
                  with_vectors: bool = False,
                  per_page: int = 10) -> List[Dict]:

        all_points = []
        offset = 0
        while offset is not None:
            points, offset = self.client.scroll(
                collection_name=self.collection_name,
                limit=per_page,
                offset=offset,
                scroll_filter=self.create_filter(sources=sources,
                                                 categories=categories,
                                                 scopes=scopes,
                                                 contexts=contexts),
                with_payload=with_payload,
                with_vectors=with_vectors)
            all_points += [point.payload for point in points]

        return all_points
