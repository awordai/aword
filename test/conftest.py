# -*- coding: utf-8 -*-

import os
import pytest

from aword.app import Awd


@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['AWORD_CONFIG'] = 'test/res'


@pytest.fixture(scope='module')
def awd():
    return Awd()


@pytest.fixture(scope='module')
def resdir():
    return 'test/res'


@pytest.fixture(scope='module')
def collection_name():
    return 'test-collection'


# pylint: disable=redefined-outer-name
@pytest.fixture(scope='module')
def ensure_empty_collection(awd, collection_name):
    store = awd.get_store(collection_name)
    store.client.delete_collection(collection_name)
    awd.create_store_collection(collection_name)
