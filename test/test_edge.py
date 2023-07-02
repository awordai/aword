# -*- coding: utf-8 -*-

from datetime import datetime
from dateutil.relativedelta import relativedelta

import aword.cache.edge as E
from aword.segment import Segment
from aword.vdbfields import VectorDbFields


def test_add_and_get():
    su = E.SourceUnit(in_memory=True)
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
           last_edited_timestamp=datetime.now(),
           summary='test_summary',
           segments=segments,
           **vector_db_fields)

    result = su.get(vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value])

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
    su = E.SourceUnit(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id'
    }

    editor_2 = 'test_editor_2'
    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by=editor_2,
           last_edited_timestamp=datetime.now(),
           summary='test_summary 2',
           segments=[], **vector_db_fields)

    result = su.get_by_uri('file://test_uri')

    assert result['source_unit_id'] == vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value]
    assert result['source'] == vector_db_fields[VectorDbFields.SOURCE.value]
    assert result['last_edited_by'] == editor_2


def test_get_unembedded():
    su = E.SourceUnit(in_memory=True)
    vector_db_fields = {
        VectorDbFields.SOURCE.value: 'test_source',
        VectorDbFields.SOURCE_UNIT_ID.value: 'test_id_3'
    }
    su.add(uri='file://test_uri',
           created_by='test_creator',
           last_edited_by='editor',
           last_edited_timestamp=datetime.now(),
           last_embedded_timestamp=datetime.now() - relativedelta(days=1),
           summary='test_summary 3',
           segments=[], **vector_db_fields)

    results = su.get_unembedded()

    assert len(results) == 2
    for result in results:
        assert (result['last_embedded_timestamp'] is None or
                result['last_embedded_timestamp'] < result['last_edited_timestamp'])
