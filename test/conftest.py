# -*- coding: utf-8 -*-

import os
import pytest
import tiktoken

import aword.tools as T


@pytest.fixture(autouse=True)
def set_env_vars():
    os.environ['AWORD_CONFIG'] = 'res/test/config.ini'
    os.environ['AWORD_SOURCES_CONFIG'] = 'res/test/sources.json'


@pytest.fixture(scope='session')
def tokenizer():
    os.environ['AWORD_CONFIG'] = 'res/test/config.ini'
    C = T.get_config('openai')
    tk = tiktoken.get_encoding(C['oai_embedding_encoding'])
    yield tk
