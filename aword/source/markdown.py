# -*- coding: utf-8 -*-

from datetime import datetime
from typing import Any, Dict

import mistune
from mistune.renderers.markdown import MarkdownRenderer

import aword.tools as T
from aword.payload import Segment


class CustomMarkdownRenderer(MarkdownRenderer):
    def __init__(self):
        super().__init__()
        self.current_heading_chain = []
        self.segments = []

    def heading(self, token, state) -> str:
        level = token['attrs']['level']
        text = self.render_children(token, state)

        self.current_heading_chain = self.current_heading_chain[:level - 1]
        self.current_heading_chain.append(text)
        return ''

    def paragraph(self, token, state) -> str:
        text = self.render_children(token, state)
        self.segments.append(Segment(body=text,
                                     headings=list(self.current_heading_chain)))
        return ''


def parse(file_path: str,
          uri: str,
          source: str,
          author: str,
          category: str,
          scope: str,
          timestamp: datetime,
          metadata: Dict[str, Any] = None):

    with open(file_path, 'r', encoding='utf-8') as file:
        markdown_text = file.read()

    renderer = CustomMarkdownRenderer()
    mistune.create_markdown(renderer=renderer)(markdown_text)

    anchors_so_far = {}

    out = renderer.segments
    for segment in out:
        segment.source_unit_id = uri

        anchor, anchors_so_far = T.generate_anchor(segment.headings, anchors_so_far)
        segment.uri = uri + anchor
        segment.created_by = author
        segment.source = source
        segment.category = category
        segment.scope = scope

        segment.last_edited_timestamp = timestamp
        segment.metadata = (metadata or {}).copy()

    return out


def main():
    fname = 'res/test/local/butterfly-biology.md'
    segments = parse(fname,
                     T.file_to_uri(fname),
                     source='local:altair.local',
                     author='Author',
                     category='Reference',
                     scope='confidential',
                     timestamp=T.timestamp_as_utc())

    from pprint import pprint
    pprint(segments)


if __name__ == '__main__':
    main()
