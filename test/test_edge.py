# -*- coding: utf-8 -*-

from datetime import datetime, timezone

import aword.cache.edge as E


def test_source_unit():
    # Create a new database in memory and a SourceUnit object
    conn = E.create_connection()
    source_unit = E.SourceUnit(conn)

    # Test adding a source_unit
    now = datetime.now(timezone.utc)
    source_unit.add_source_unit('id1',
                                'notion',
                                'uri1',
                                'creator1',
                                'editor1',
                                now,
                                now,
                                'reference',
                                'public',
                                'summary1',
                                'body1',
                                'metadata1')

    # Test retrieving by ID
    result = source_unit.get('id1')
    assert result['source_unit_id'] == 'id1'
    assert result['source'] == 'notion'
    assert result['uri'] == 'uri1'
    assert result['created_by'] == 'creator1'
    assert result['last_edited_by'] == 'editor1'
    assert result['last_edited_timestamp'] == now
    assert result['last_embedded_timestamp'] == now
    assert result['fact_type'] == 'reference'
    assert result['scope'] == 'public'
    assert result['summary'] == 'summary1'
    assert result['body'] == 'body1'
    assert result['metadata'] == 'metadata1'

    # Test retrieving by URI
    result = source_unit.get_by_uri('uri1')
    assert result['source_unit_id'] == 'id1'
    assert result['source'] == 'notion'
    assert result['uri'] == 'uri1'
    assert result['created_by'] == 'creator1'
    assert result['last_edited_by'] == 'editor1'
    assert result['last_edited_timestamp'] == now
    assert result['last_embedded_timestamp'] == now
    assert result['fact_type'] == 'reference'
    assert result['scope'] == 'public'
    assert result['summary'] == 'summary1'
    assert result['body'] == 'body1'
    assert result['metadata'] == 'metadata1'


def test_section():
    # Create a new database in memory and a Section object
    conn = E.create_connection()
    section = E.Section(conn)

    now = datetime.now(timezone.utc)
    headings = ['heading1', 'heading2']
    metadata = {'something': 1,
                'something_else': False}
    section_id = section.add_section('source_unit_id1',
                                     'uri1',
                                     'creator1',
                                     'editor1',
                                     now,
                                     now,
                                     'body1',
                                     headings,
                                     metadata)

    # Test retrieving by ID
    result = section.get(section_id)
    assert result['source_unit_id'] == 'source_unit_id1'
    assert result['uri'] == 'uri1'
    assert result['created_by'] == 'creator1'
    assert result['last_edited_by'] == 'editor1'
    assert result['last_edited_timestamp'] == now
    assert result['last_embedded_timestamp'] == now
    assert result['body'] == 'body1'
    assert result['headings'] == headings
    assert result['metadata'] == metadata

    # Test retrieving by URI
    result = section.get_by_uri('uri1')
    assert result['section_id'] == section_id
    assert result['source_unit_id'] == 'source_unit_id1'
    assert result['uri'] == 'uri1'
    assert result['created_by'] == 'creator1'
    assert result['last_edited_by'] == 'editor1'
    assert result['last_edited_timestamp'] == now
    assert result['last_embedded_timestamp'] == now
    assert result['body'] == 'body1'
    assert result['headings'] == headings
    assert result['metadata'] == metadata


def test_chunk():
    # Create a new database in memory and a Chunk object
    conn = E.create_connection()
    chunk = E.Chunk(conn)

    # Test adding a chunk
    vector_embedding = [float(i) for i in range(1536)]  # your vector embedding
    chunk.add_chunk('chunk_id1', 'section_id1', 'body1', vector_embedding)

    # Test retrieving by ID
    result = chunk.get('chunk_id1')
    assert result['chunk_id'] == 'chunk_id1'
    assert result['section_id'] == 'section_id1'
    assert result['body'] == 'body1'
    assert result['vector_embedding'] == vector_embedding
