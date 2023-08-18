# -*- coding: utf-8 -*-

import time

from aword.source.notion import ensure_api_key, process_page, add_to_cache


def test_process_page(awd):
    suc = awd.get_source_unit_cache()
    ensure_api_key(awd)
    segments = process_page(page_id='414544fd1d534621a89e467f1b94f650',
                            source_unit_cache=suc,
                            max_break_level=2)
    from pprint import pprint
    pprint(segments)
    assert len(segments) == 3

    # assert segments[0]['body'] ==


def _test_add_to_cache(awd):
    suc = awd.get_source_unit_cache()
    suc.reset_tables()

    segments = add_to_cache(awd)
    assert len(segments) == 21

    time.sleep(0.5)

    segments = add_to_cache(awd)
    assert len(segments) == 0
