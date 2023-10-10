# -*- coding: utf-8 -*-

import pytest

from aword.app import Awd
from aword.logger import configure_logging


def pytest_addoption(parser):
    parser.addoption("--silent", action="store_true", default=False, help="Silence info logging")


def pytest_configure(config):
    # Read the command line options
    debug = config.getoption("--debug")
    silent = config.getoption("--silent")

    # Configure logging
    configure_logging(debug=debug, silent=silent)


@pytest.fixture(scope='module')
def awd():
    return Awd(environment_name='test',
               config_dir='test/res')


@pytest.fixture(scope='module')
def resdir():
    return 'test/res'


# pylint: disable=redefined-outer-name
@pytest.fixture(scope='module')
def ensure_empty_vector_namespace(awd):
    store = awd.get_vector_store()
    store.delete_namespace()
    awd.create_vector_namespace()
