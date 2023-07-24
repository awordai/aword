# -*- coding: utf-8 -*-

import uuid
import time
from datetime import datetime
from pytz import utc
from dateutil.relativedelta import relativedelta

import aword.cache.edge as E
from aword.segment import Segment
from aword.chunk import Payload, Chunk


def test_add_and_get():
    su = E.SourceUnitDB()
    su.reset_tables()
    source = 'test_source'
    source_unit_id = 'test_id'

    segment_1 = Segment('body1', uri='http://uri1', created_by='creator1')
    segment_2 = Segment('body2', uri='http://uri2', created_by='creator2')
    segments = [segment_1, segment_2]

    metadata = {'key': 'value'}

    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri',
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=datetime.now(utc),
                     summary='test_summary',
                     segments=segments,
                     metadata=metadata)

    result = su.get(source, source_unit_id)

    assert result['source_unit_id'] == source_unit_id
    assert result['source'] == source
    assert result['uri'] == 'file://test_uri'
    assert result['created_by'] == 'test_creator'
    assert result['last_edited_by'] == 'test_editor'
    assert result['summary'] == 'test_summary'
    assert result['metadata'] == metadata

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
    su = E.SourceUnitDB()
    source = 'test_source'
    source_unit_id = 'test_id'
    uri = 'file://test_uri'

    editor_2 = 'test_editor_2'
    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri=uri,
                     created_by='test_creator',
                     last_edited_by=editor_2,
                     last_edited_timestamp=datetime.now(utc),
                     summary='test_summary 2',
                     segments=[])

    result = su.get_by_uri(source, uri)

    assert result['source_unit_id'] == source_unit_id
    assert result['source'] == source
    assert result['last_edited_by'] == editor_2


def test_get_unembedded():
    su = E.SourceUnitDB()
    source = 'test_source'
    source_unit_id = 'test_id_3'

    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri',
                     created_by='test_creator',
                     last_edited_by='editor',
                     last_edited_timestamp=datetime.now(utc) - relativedelta(hours=2),
                     summary='test_summary 3',
                     segments=[])

    results = su.get_unembedded()
    assert len(results) == 2

    su.flag_as_embedded(results)

    results = su.get_unembedded()
    assert len(results) == 0


def test_update_and_history():
    su = E.SourceUnitDB()
    source = 'test_source'
    source_unit_id = 'test_id_3'

    summary_to_modify = 'test_summary to modify'
    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri',
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=datetime.now(utc) - relativedelta(seconds=10),
                     summary=summary_to_modify,
                     segments=[])

    result_before_update = su.get(source, source_unit_id)

    summary_modified = 'test_summary modified'
    time.sleep(0.1)
    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri_updated',
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=datetime.now(utc),
                     summary=summary_modified,
                     segments=[])

    result_after_update = su.get(source, source_unit_id)

    assert result_before_update != result_after_update
    assert result_after_update['summary'] == summary_modified

    history = su.get_history(source, source_unit_id)
    assert history[0]['summary'] == summary_to_modify


def test_different_sources():
    su = E.SourceUnitDB()
    source_1 = 'test_source_1'
    source_2 = 'test_source_2'
    source_unit_id = 'test_id'

    su.add_or_update(source=source_1,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri',
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=datetime.now(utc),
                     summary='test_summary',
                     segments=[])

    su.add_or_update(source=source_2,
                     source_unit_id=source_unit_id,
                     uri='file://test_uri',
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=datetime.now(utc),
                     summary='test_summary',
                     segments=[])

    result_source_1 = su.get(source_1, source_unit_id)
    result_source_2 = su.get(source_2, source_unit_id)

    assert result_source_1 != result_source_2


def test_recreate_state():
    su = E.SourceUnitDB()
    su.reset_tables()
    source = 'test_source'
    source_unit_id = 'test_id'

    timestamp_2 = datetime.now(utc)
    timestamp_1 = timestamp_2 - relativedelta(seconds=10)

    first_uri = 'file://test_uri_1'
    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri=first_uri,
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=timestamp_1,
                     summary='test_summary',
                     segments=[])

    second_uri = 'file://test_uri_2'
    su.add_or_update(source=source,
                     source_unit_id=source_unit_id,
                     uri=second_uri,
                     created_by='test_creator',
                     last_edited_by='test_editor',
                     last_edited_timestamp=timestamp_2,
                     summary='test_summary',
                     segments=[])

    state_at_timestamp_1 = su.get_state_at_date(timestamp_1 + relativedelta(seconds=1))
    assert len(state_at_timestamp_1) == 1
    assert state_at_timestamp_1[0]['uri'] == first_uri

    state_at_timestamp_2 = su.get_state_at_date(timestamp_2 + relativedelta(seconds=1))
    assert len(state_at_timestamp_2) == 1
    assert state_at_timestamp_2[0]['uri'] == second_uri


def test_chunk_add_and_get():
    db = E.ChunkDB('test_model')
    source = "test_source"
    source_unit_id = "test_source_unit_id"
    text = "test_text"
    vector = [1, 2, 3]
    vector_db_id = "test_vector_db_id"
    chunk_id = str(uuid.uuid5(uuid.NAMESPACE_URL, text))
    chunks = [Chunk(payload=Payload(body=text),
                    vector=vector,
                    vector_db_id=vector_db_id,
                    chunk_id=chunk_id)]

    now = datetime.now(utc)
    db.add(source, source_unit_id, chunks, now)

    result = db.get(chunk_id)
    assert result.payload.body == text
    assert result.vector == vector
    assert result.vector_db_id == vector_db_id

    most_recent_datetime = db.get_most_recent_addition_datetime()
    assert most_recent_datetime == now


def test_get_non_existent():
    db = E.ChunkDB('test_model')
    result = db.get("non_existent_chunk_id")
    assert result is None
