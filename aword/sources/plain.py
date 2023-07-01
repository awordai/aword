# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Any, Dict

from aword.payload import Segment


def parse(file_path: str,
          uri: str,
          source: str,
          author: str,
          category: str,
          scope: str,
          timestamp: datetime,
          metadata: Dict[str, Any] = None):

    """Plain text files return a single chunk, it will be further
    chunked during embedding if required.
    """
    with open(file_path, 'r', encoding='utf-8') as fin:
        return [Segment(body=fin.read().strip(),
                        source=source,
                        source_unit_id=uri,
                        category=category,
                        scope=scope,
                        uri=uri,
                        headings=[],
                        created_by=author,
                        last_edited_timestamp=timestamp,
                        metadata=(metadata or {}).copy())]
