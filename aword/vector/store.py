# -*- coding: utf-8 -*-

import uuid
from typing import List, Dict, Union
from abc import ABC, abstractmethod

from qdrant_client import models
from qdrant_client import QdrantClient
from qdrant_client.http.models import Filter, FieldCondition, MatchValue, PointStruct

from aword.model.embedder import Embedder
from aword.chunk import Chunk, Payload
from aword.segment import Segment


def make_store(awd,
               collection_name: str,
               config: Dict):
    provider = config.get('provider', 'qdrant')
    if provider == 'qdrant':
        # collection_name can come in the config, overriding it if so.
        return QdrantStore(awd, **{**config, **{'collection_name': collection_name}})

    raise ValueError(f'Unknown vector store provider {provider}')


def make_id(source_unit_id, text):
    return str(uuid.uuid5(uuid.NAMESPACE_X500, source_unit_id + text))


class Store(ABC):

    def __init__(self, awd):
        self.logger = awd.logger

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
                          scope: str,  # confidential, public
                          context: str,  # historical, reference, internal_comm...
                          language: str,
                          segments: List[Segment]) -> List[Chunk]:

        to_upsert = []
        for segment in segments:
            # TODO Maybe it should include the headings in the
            # embedding.  Maybe do the embedding separately, and then
            # average it with a weight. It will probably not work with
            # non normalized embeddings.

            chunks = embedder.get_embedded_chunks(segment.body)
            self.logger.info('Embedding (%s, %s), got %s chunks',
                             source,
                             source_unit_id,
                             len(chunks))
            for chunk in chunks:
                chunk.payload.source = source
                chunk.payload.source_unit_id = source_unit_id
                chunk.payload.categories = categories
                chunk.payload.scope = scope
                chunk.payload.context = context
                chunk.payload.language = segment.language or language
                chunk.payload.headings = segment.headings
                chunk.payload.created_by = segment.created_by
                chunk.payload.last_edited_by = segment.last_edited_by
                chunk.payload.last_edited_timestamp = segment.last_edited_timestamp
                chunk.payload.metadata = segment.metadata

                to_upsert.append(chunk)

        self.clean_source_unit(source_unit_id)
        return self.upsert_chunks(to_upsert)


class QdrantStore(Store):

    def __init__(self,
                 awd,
                 collection_name,
                 local_db: str = None,
                 url: str = None,
                 distance: str = 'cosine',
                 **_):
        super().__init__(awd)
        self.collection_name = collection_name
        self.distance = distance

        if url:
            client_pars = {'url': url}
            if 'localhost' not in url and '127.0.0' not in url:
                client_pars['api_key'] = awd.getenv('QDRANT_API_KEY')
            self.logger.info('Connecting to remote qdrant client on %s', url)
        elif local_db:
            client_pars = {'path': local_db}
            self.logger.info('Connecting to local qdrant client on %s', local_db)
        else:
            raise RuntimeError('Need either local_db or url')

        self.client = QdrantClient(**client_pars)
        try:
            self.client.get_collection(collection_name=collection_name)
        except:
            self.logger.warn('Collection %s does not exist, creating it', collection_name)
            self.create_collection(dimensions=awd.get_embedder().dimensions)

    def create_collection(self, dimensions: int):
        distance = {'dot': models.Distance.DOT,
                    'cosine': models.Distance.COSINE,
                    'euclid': models.Distance.EUCLID}[self.distance]
        self.client.recreate_collection(
            collection_name=self.collection_name,
            vectors_config=models.VectorParams(size=dimensions,
                                               distance=distance))

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='source',
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='source_unit_id',
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='categories',
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='scope',
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='context',
                                         field_schema="keyword")

        self.client.create_payload_index(collection_name=self.collection_name,
                                         field_name='language',
                                         field_schema="keyword")

        self.logger.info('Created collection %s', self.collection_name)

    def create_filter(self,
                      sources: Union[List[str], str] = None,
                      source_unit_ids: Union[List[str], str] = None,
                      categories: Union[List[str], str] = None,
                      scopes: Union[List[str], str] = None,
                      contexts: Union[List[str], str] = None,
                      languages: Union[List[str], str] = None) -> models.Filter:
        """The filter possibilities are: source, categories, scope and
        context. If more than one value comes for each, any of the
        values should match. If more than one filter comes all should
        be applied.

        """
        where = {}
        if sources:
            where['source'] = sources
        if source_unit_ids:
            where['source_unit_id'] = source_unit_ids
        if categories:
            where['categories'] = categories
        if scopes:
            where['scope'] = scopes
        if contexts:
            where['context'] = contexts
        if languages:
            where['language'] = languages

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
               source_unit_ids: Union[List[str], str] = None,
               categories: Union[List[str], str] = None,
               scopes: Union[List[str], str] = None,
               contexts: Union[List[str], str] = None,
               languages: Union[List[str], str] = None) -> List[Payload]:

        out = self.client.search(collection_name=self.collection_name,
                                 query_vector=query_vector,
                                 query_filter=self.create_filter(sources=sources,
                                                                 source_unit_ids=source_unit_ids,
                                                                 categories=categories,
                                                                 scopes=scopes,
                                                                 contexts=contexts,
                                                                 languages=languages),
                                 limit=limit)
        return [Payload(**(r.payload)) for r in out]

    def count(self,
              sources: Union[List[str], str] = None,
              source_unit_ids: Union[List[str], str] = None,
              categories: Union[List[str], str] = None,
              scopes: Union[List[str], str] = None,
              contexts: Union[List[str], str] = None,
              languages: Union[List[str], str] = None) -> int:
        return self.client.count(collection_name=self.collection_name,
                                 count_filter=self.create_filter(sources=sources,
                                                                 source_unit_ids=source_unit_ids,
                                                                 categories=categories,
                                                                 scopes=scopes,
                                                                 contexts=contexts,
                                                                 languages=languages),
                                 exact=True).count


    def clean_source_unit(self, source_unit_id: str):
        self.client.delete(collection_name=self.collection_name,
                           points_selector=Filter(must=[
                               FieldCondition(key='source_unit_id',
                                              match=MatchValue(value=source_unit_id))]))

    def upsert_chunks(self, chunks: List[Chunk]) -> List[Chunk]:
        """Uploads and inserts chunks to the vector database. It
        returns a copy of the list of chunks, where each chunk
        includes a vector_db_id field with a newly generated id with
        which it can be retrieved from the vector database.
        """
        out = []
        points = []
        for chunk in chunks:
            vector_db_id = make_id(chunk.payload.source + chunk.payload.source_unit_id,
                                   chunk.payload.body)
            points.append(PointStruct(id=vector_db_id,
                                      vector=chunk.vector,
                                      payload=chunk.payload))

            out_chunk = chunk.copy()
            out_chunk.vector_db_id = vector_db_id
            out.append(out_chunk)

        self.client.upsert(collection_name=self.collection_name,
                           points=points)
        self.logger.info('Upserted %s points to %s', len(points), self.collection_name)
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
                  source_unit_ids: Union[List[str], str] = None,
                  categories: Union[List[str], str] = None,
                  scopes: Union[List[str], str] = None,
                  contexts: Union[List[str], str] = None,
                  languages: Union[List[str], str] = None,
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
                                                 source_unit_ids=source_unit_ids,
                                                 categories=categories,
                                                 scopes=scopes,
                                                 contexts=contexts,
                                                 languages=languages),
                with_payload=with_payload,
                with_vectors=with_vectors)
            all_points += [point.payload for point in points]

        return all_points


def add_args(parser):
    parser.add_argument('--source',
                        help='Limit listings and actions to this source.',
                        type=str)
    parser.add_argument('--source-unit-id',
                        help=('Limit listings and actions to this source unit id. '
                              'It only makes sense if source is also defined with --source.'),
                        type=str)

    parser.add_argument('--count-embeddings',
                        help=('Count the embeddings.'),
                        action='store_true')


def main(awd, args):
    if args['source_unit_id'] and not args['source']:
        awd.logger.error('Got source-unit-id but no source, please specify one.')
        import sys
        sys.exit(1)

    filter_args = {}
    if args['source']:
        filter_args['sources'] = [args['source']]
    if args['source_unit_id']:
        filter_args['source_unit_ids'] = [args['source_unit_id']]

    store = awd.get_store()
    if args['count_embeddings']:
        print(store.count(**filter_args))
