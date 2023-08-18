# -*- coding: utf-8 -*-

import time

from aword.source.notion import ensure_api_key, process_page, add_to_cache


def test_process_page(awd):
    suc = awd.get_source_unit_cache()
    ensure_api_key(awd)
    # https://www.notion.so/Headings-parsing-test-414544fd1d534621a89e467f1b94f650
    segments = process_page(page_id='414544fd1d534621a89e467f1b94f650',
                            source_unit_cache=suc,
                            max_break_level=2)

    assert segments[0]['body'] == ('- First heading 1\n'
                                   'Some text right after heading 1\n'
                                   '- This is a heading 2\n'
                                   'Some text in heading 2\n'
                                   '- A heading 3\n'
                                   'Some text in heading 3\n'
                                   '- Another heading 3\n'
                                   'Some text in the second heading 3')
    assert segments[0]['headings'] == ['Headings parsing test']

    assert segments[1]['body'] == 'Text in the second heading 2'
    assert segments[1]['headings'] == ['Headings parsing test', 'Another heading 2']

    assert segments[2]['body'] == ('- A heading 2 in the second heading 1\n'
                                   'Text in the heading 2 of the second heading 1\n'
                                   'Some more text.\n'
                                   '- A heading 3 in the heading 2 of the '
                                   'second heading 1\n'
                                   'With some more text')
    assert segments[2]['headings'] == ['Headings parsing test', 'Second heading 1']


def test_add_to_cache(awd):
    suc = awd.get_source_unit_cache()
    suc.reset_tables()

    segments = add_to_cache(awd)
    assert len(segments) == 23

    time.sleep(0.5)

    segments = add_to_cache(awd)
    assert len(segments) == 0
