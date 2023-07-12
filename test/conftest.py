# -*- coding: utf-8 -*-

import os
import pytest

from aword.config import Awd


@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['AWORD_CONFIG'] = 'test/res'


@pytest.fixture(scope="module")
def awd():
    return Awd()

@pytest.fixture(scope="module")
def collection_name():
    return 'test-collection'
