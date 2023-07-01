# -*- coding: utf-8 -*-

import os

from qdrant_client import QdrantClient
from qdrant_client.models import Distance, VectorParams
from qdrant_client.http.models import Filter, FieldCondition, MatchValue

import aword.tools as T

QClient = None


def get_qdrant_client():
    global QClient
    if QClient is None:
        T.load_environment()
        C = T.get_config('qdrant')
        if C['qdrant_local_db']:
            client_pars = {'path': C['qdrant_local_db']}
        elif C['qdrant_url']:
            client_pars = {'url': C['qdrant_local_db'],
                           'api_key': os.environ['QDRANT_API_KEY']}
        else:
            raise RuntimeError('Need either qdrant_local_db or '
                               'qdrant_local_db in config')

        QClient = QdrantClient(**client_pars)

    return QClient


def create_collection():
    client = get_qdrant_client()

    C = T.get_config('qdrant')
    C_oai = T.get_config('openai')

    client.recreate_collection(
        collection_name=C['qdrant_collection'],
        vectors_config=VectorParams(size=C_oai['oai_embedding_dimensions'],
                                    distance=Distance.COSINE))

    client.create_payload_index(collection_name=C['qdrant_collection'],
                                field_name=C['qdrant_suid_field'],
                                field_schema="keyword")
    client.create_payload_index(collection_name=C['qdrant_collection'],
                                field_name=C['qdrant_source_field'],
                                field_schema="keyword")

    # TODO there can be several scopes (it's actually an array), check that the schema is keyword.
    client.create_payload_index(collection_name=C['qdrant_collection'],
                                field_name=C['qdrant_scope_field'],
                                field_schema="keyword")

    client.create_payload_index(collection_name=C['qdrant_collection'],
                                field_name=C['qdrant_category_field'],
                                field_schema="keyword")


def count():
    client = get_qdrant_client()

    C = T.get_config('qdrant')
    return client.count(collection_name=C['qdrant_collection'],
                        exact=True).count


def search(query_vector, limit=5):
    client = get_qdrant_client()

    C = T.get_config('qdrant')
    out = client.search(collection_name=C['qdrant_collection'],
                        query_vector=query_vector,
                        limit=limit)
    return [r.payload for r in out]


def clean_source_unit(source_unit_id):
    # We assume that when a source unit is embedded it will replace
    # any previous version of the same unit, so we first need to
    # delete all the points belonging to this source_unit_id if any
    # exists
    client = get_qdrant_client()

    C = T.get_config('qdrant')
    client.delete(collection_name=C['qdrant_collection'],
                  points_selector=Filter(must=[
                      FieldCondition(key=C['qdrant_suid_field'],
                                     match=MatchValue(value=source_unit_id))]))
