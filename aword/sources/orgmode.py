# -*- coding: utf-8 -*-

import re
from datetime import datetime
from typing import Any, Dict

import aword.tools as T
from aword.payload import Payload, FactType


def parse(file_path: str,
          uri: str,
          source: str,
          author: str,
          fact_type: FactType,
          timestamp: datetime,
          metadata: Dict[str, Any] = None):

    with open(file_path, 'r', encoding='utf-8') as file:
        org_lines = file.readlines()

    current_heading_chain = []
    current_text = []
    chunks = []
    anchors_so_far = {}

    for line in org_lines:
        heading_match = re.match(r'(\*+)\s(.+)', line)
        if heading_match:
            level = len(heading_match.group(1))
            heading = heading_match.group(2).strip()

            # If there's text accumulated, save it as a new chunk
            text_so_far = ' '.join(current_text).strip()
            if text_so_far:
                anchor, anchors_so_far = T.generate_anchor(current_heading_chain,
                                                           anchors_so_far)
                chunks.append(
                    Payload(body=text_so_far,
                            source_unit_id=uri,
                            uri=uri + anchor,
                            headings=list(current_heading_chain),
                            created_by=author,
                            source=source,
                            fact_type=fact_type,
                            timestamp=timestamp,
                            metadata=(metadata or {}).copy()))
                current_text = []

            # Adjust the current heading chain to match the new level and heading
            current_heading_chain = current_heading_chain[:level - 1]
            current_heading_chain.append(heading)
        else:
            current_text.append(line.strip())

    # Don't forget the last chunk
    if current_text:
        anchor, _ = T.generate_anchor(current_heading_chain, anchors_so_far)

        chunks.append(
            Payload(body=' '.join(current_text).strip(),
                    source_unit_id=uri,
                    uri=uri + anchor,
                    headings=list(current_heading_chain),
                    created_by=author,
                    source=source,
                    fact_type=fact_type,
                    timestamp=timestamp,
                    metadata=(metadata or {}).copy()))

    return chunks
