# -*- coding: utf-8 -*-

from enum import Enum


class VectorDbFields(Enum):
    SOURCE = 'source'
    SOURCE_UNIT_ID = 'source_unit_id'
    BODY = 'body'
    CATEGORIES = 'categories'
    SCOPE = 'scope'
    CONTEXT = 'context'
    LANGUAGE = 'language'
