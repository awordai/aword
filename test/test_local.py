# -*- coding: utf-8 -*-

import os
import time

from aword.source.local import add_to_cache


def update_modification_time(directory):
    for dirpath, _, filenames in os.walk(directory):
        for filename in filenames:
            file_path = os.path.join(dirpath, filename)
            current_time = time.time()
            os.utime(file_path, (current_time, current_time))


def test_local(awd):
    suc = awd.get_source_unit_cache()
    suc.reset_tables()

    segments = add_to_cache(awd)
    assert len(segments) == 39

    time.sleep(0.5)

    segments = add_to_cache(awd)
    assert len(segments) == 0
