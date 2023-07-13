# -*- coding: utf-8 -*-

import os
import time
import socket

import aword.tools as T
from aword.chat import format_context


def update_modification_time(directory):
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            current_time = time.time()
            os.utime(file_path, (current_time, current_time))


def _test_prompt():
    q_client = qdrant.get_qdrant_client()

    config = T.get_config('qdrant')
    q_client.delete_collection(collection_name=config['qdrant_collection'])
    qdrant.create_collection()

    source_config = T.get_source_config('local')

    # Directory with a single non-empty md file.
    test_dir = source_config[1]['directory']
    update_modification_time(test_dir)

    ingest(only_in_directory=test_dir)
    assert qdrant.count() == 1

    all_context = qdrant.search(oai.get_embeddings(['Spain'])[0], limit=1)
    assert len(all_context) == 1

    assert set(all_context[0].keys()) == set(['body',
                                              'fact_type',
                                              'headings',
                                              'metadata',
                                              'created_by',
                                              'last_edited_by',
                                              'source',
                                              'source_unit_id',
                                              'timestamp',
                                              'uri'])

    timestamp = T.timestamp_as_utc().isoformat()
    all_context[0]['timestamp'] = timestamp

    hostname = socket.gethostname()

    abs_test_dir = os.path.abspath(test_dir)
    assert format_context(all_context[0]) == (
        '```\n'
        f'url_or_file: {abs_test_dir}/galicia.md\n'
        f'source: local:{hostname}\n'
        'created_by: John Doe\n'
        'fact_type: historical\n'
        f'timestamp: {timestamp}\n'
        'breadcrumbs: Galicia > Introduction\n'
        'body: It is the north-western corner of the Iberian Peninsula. Great food, '
        'nice people, and amazing views.\n'
        '```')
