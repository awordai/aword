# -*- coding: utf-8 -*-

import uuid
from datetime import datetime
from pytz import utc
from dateutil.relativedelta import relativedelta

import aword.cache.edge as E
from aword.segment import Segment
from aword.chunk import Chunk
from aword.vdbfields import VectorDbFields


def test_add_and_get():
    su = E.SourceUnitDB(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    segment_1 = Segment('body1', uri='http://uri1', created_by='creator1')
    segment_2 = Segment('body2', uri='http://uri2', created_by='creator2')
    segments = [segment_1, segment_2]

    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=datetime.now(utc),
           summary='test_summary',
           segments=segments,
           **vector_db_fields)

    result = su.get(vector_db_fields[VectorDbFields.SOURCE.value],
                    vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value])

    assert result['source_unit_id'] == vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value]
    assert result['source'] == vector_db_fields[VectorDbFields.SOURCE.value]
    assert result['uri'] == 'file://test_uri'
    assert result['created_by'] == 'test_creator'
    assert result['last_edited_by'] == 'test_editor'
    assert result['summary'] == 'test_summary'

    returned_segments = result['segments']
    assert len(returned_segments) == 2

    assert isinstance(returned_segments[0], Segment)
    assert returned_segments[0]['body'] == 'body1'
    assert returned_segments[0]['uri'] == 'http://uri1'
    assert returned_segments[0]['created_by'] == 'creator1'

    assert isinstance(returned_segments[1], Segment)
    assert returned_segments[1]['body'] == 'body2'
    assert returned_segments[1]['uri'] == 'http://uri2'
    assert returned_segments[1]['created_by'] == 'creator2'


def test_get_by_uri():
    su = E.SourceUnitDB(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    editor_2 = 'test_editor_2'
    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by=editor_2,
           last_edited_timestamp=datetime.now(utc),
           summary='test_summary 2',
           segments=[], **vector_db_fields)

    result = su.get_by_uri(vector_db_fields[VectorDbFields.SOURCE.value],
                           'file://test_uri')

    assert result['source_unit_id'] == vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value]
    assert result['source'] == vector_db_fields[VectorDbFields.SOURCE.value]
    assert result['last_edited_by'] == editor_2


def test_get_unembedded():
    su = E.SourceUnitDB(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id_3'
    }
    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='editor',
           last_edited_timestamp=datetime.now(utc),
           added_timestamp=datetime.now(utc) - relativedelta(days=1),
           summary='test_summary 3',
           segments=[], **vector_db_fields)

    last_embedded_timestamp = last_embedded_timestamp=datetime.now(utc) - relativedelta(days=2)
    results = su.get_unembedded(last_embedded_timestamp)

    assert len(results) == 2
    for result in results:
        assert result['added_timestamp'] < result['last_edited_timestamp']


def test_update_and_history():
    su = E.SourceUnitDB(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    summary_to_modify = 'test_summary to modify'
    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=datetime.now(utc) - relativedelta(seconds=10),
           summary=summary_to_modify,
           segments=[],
           **vector_db_fields)

    result_before_update = su.get(vector_db_fields[VectorDbFields.SOURCE.value],
                                  vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value])

    summary_modified = 'test_summary modified'
    su.add(uri='file://test_uri_updated',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=datetime.now(utc),
           summary=summary_modified,
           segments=[],
           **vector_db_fields)

    result_after_update = su.get(vector_db_fields[VectorDbFields.SOURCE.value],
                                 vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value])

    assert result_before_update != result_after_update
    assert result_after_update['summary'] == summary_modified

    history = su.get_history(vector_db_fields[VectorDbFields.SOURCE.value],
                             vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value])
    assert history[-1]['summary'] == summary_to_modify


def test_different_sources():
    su = E.SourceUnitDB(in_memory=True)
    vector_db_fields_source_1 = {
        VectorDbFields.SOURCE.value: 'test_source_1',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }
    vector_db_fields_source_2 = {
        VectorDbFields.SOURCE.value: 'test_source_2',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=datetime.now(utc),
           summary='test_summary',
           segments=[],
           **vector_db_fields_source_1)

    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=datetime.now(utc),
           summary='test_summary',
           segments=[],
           **vector_db_fields_source_2)

    result_source_1 = su.get(vector_db_fields_source_1[VectorDbFields.SOURCE.value],
                             vector_db_fields_source_1[VectorDbFields.SOURCE_UNIT_ID.value])

    result_source_2 = su.get(vector_db_fields_source_2[VectorDbFields.SOURCE.value],
                             vector_db_fields_source_2[VectorDbFields.SOURCE_UNIT_ID.value])

    assert result_source_1 != result_source_2


def test_recreate_state():
    su = E.SourceUnitDB(in_memory=True)
    su.reset_tables()
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    timestamp_1 = datetime.now(utc) - relativedelta(seconds=10)
    timestamp_2 = timestamp_1

    su.add(uri='file://test_uri_1',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=timestamp_1,
           summary='test_summary',
           segments=[],
           **vector_db_fields)

    su.add(uri='file://test_uri_2',
           created_by='test_creator',
           last_edited_by='test_editor',
           last_edited_timestamp=timestamp_2,
           summary='test_summary',
           segments=[],
           **vector_db_fields)

    state_at_timestamp_1 = su.get_state_at_date(timestamp_1 + relativedelta(seconds=1))
    assert len(state_at_timestamp_1) == 1
    assert state_at_timestamp_1[0]['uri'] == 'file://test_uri_1'

    state_at_timestamp_2 = su.get_state_at_date(timestamp_2)
    assert len(state_at_timestamp_2) == 1
    assert state_at_timestamp_2[0]['uri'] == 'file://test_uri_2'


def test_chunk_add_and_get():
    db = E.ChunkDB('test_model', in_memory=True)
    source = "test_source"
    source_unit_id = "test_source_unit_id"
    text = "test_text"
    vector = [1, 2, 3]
    vector_db_id = "test_vector_db_id"
    chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, text))
    chunks = [Chunk(text, vector, vector_db_id)]

    db.add(source, source_unit_id, chunks)

    result = db.get(chunk_id)
    assert result.text == text
    assert result.vector == vector
    assert result.vector_db_id == vector_db_id


def test_get_non_existent():
    db = E.ChunkDB('test_model', in_memory=True)
    result = db.get("non_existent_chunk_id")
    assert result is None
