# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Any, Dict

from aword.payload import Payload, FactType


def parse(file_path: str,
          uri: str,
          source: str,
          author: str,
          fact_type: FactType,
          timestamp: datetime,
          metadata: Dict[str, Any] = None):

    """Plain text files return a single chunk, it will be further
    chunked during embedding if required.
    """
    with open(file_path, 'r', encoding='utf-8') as fin:
        return [Payload(body=fin.read().strip(),
                        source_unit_id=uri,
                        uri=uri,
                        headings=[],
                        created_by=author,
                        source=source,
                        fact_type=fact_type,
                        timestamp=timestamp,
                        metadata=(metadata or {}).copy())]
