# -*- coding: utf-8 -*-
"""Parse and cache notion pages.
"""
import time
from typing import Dict, List, Tuple
import logging

import requests

import aword.errors as E
import aword.tools as T
from aword.app import Awd
from aword.segment import Segment
from aword.cache.cache import Cache


SourceName = 'notion'
Timeout = 30
Users = {}

API_KEY = ''

def ensure_api_key(awd):
    global API_KEY
    API_KEY = awd.getenv('NOTION_API_KEY')


def get_headers():
    if not API_KEY:
        raise E.AwordError('No AWORD_NOTION_API_KEY found as an environment variable')

    return {
        "Authorization": f"Bearer {API_KEY}",
        "Content-Type": "application/json",
        "Notion-Version": "2022-06-28",
    }


def fetch_all_pages_in_database(database_id: str) -> List:
    url = f"https://api.notion.com/v1/databases/{database_id}/query"
    has_more = True
    next_cursor = None

    all_pages = []
    while has_more:
        data = {
            "page_size": 20,  # Maximum allowed by the API
        }
        if next_cursor:
            data["start_cursor"] = next_cursor

        response = requests.post(url,
                                 headers=get_headers(),
                                 json=data,
                                 timeout=Timeout)

        if response.status_code != 200:
            raise E.AwordFetchError('fetch_all_pages_in_database failed.', response)

        result = response.json()
        all_pages.extend(result["results"])
        has_more = result.get("has_more", False)
        next_cursor = result.get("next_cursor", None)

    return all_pages


def fetch_block_children(block_id: str,
                         sleeping: int = 0,
                         seen_blocks: set = None) -> List:
    """It returns the actual content of the page, in the form of a
    list of blocks.
    """
    logger = logging.getLogger(__name__)
    logger.info('Fetching block/page content for %s', block_id)
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    response = requests.get(url, headers=get_headers(), timeout=Timeout)

    if response.status_code != 200:
        raise E.AwordFetchError('fetch_block_children failed.', response)

    content = response.json()["results"]

    time.sleep(sleeping)

    seen_blocks = seen_blocks if seen_blocks is not None else set([])
    out = []
    for block in content:
        out.append(block)

        if block['id'] not in seen_blocks:
            seen_blocks.add(block['id'])

            if block['has_children'] and block['type'] != 'child_page':
                out += fetch_block_children(block['id'], sleeping, seen_blocks)

    return out


def fetch_page(page_id: str) -> Dict:
    """The page includes timestamps and user information, and a
    'properties' object with things like the title, but not the page
    contents.
    """
    logger = logging.getLogger(__name__)
    logger.info('Fetching page %s', page_id)
    url = f"https://api.notion.com/v1/pages/{page_id}"
    response = requests.get(url, headers=get_headers(), timeout=Timeout)

    if response.status_code != 200:
        raise E.AwordFetchError('fetch_page failed.', response)

    return response.json()


def extract_page_title(page: Dict) -> str:
    """The title will be a property with a title component:

    "Title": {
        "id": "title",
        "type": "title",
        "title": [
            {
                "type": "text",
                "text": {
                    "content": "Bug bash",
                    "link": null
                },
                "annotations": {
                    "bold": false,
                },
                "plain_text": "Bug bash",
                "href": null
            }
        ]
    }

    or

    "Name": {
      "title": [
        {
          "type": "text",
          "text": {
            "content": "The title"
          }
        }
      ]
    }

    https://developers.notion.com/reference/page
    https://developers.notion.com/reference/property-value-object#title-property-values
    """
    def _get_element_text(element):
        if 'plain_text' in element:
            return element['plain_text']
        if 'text' in element:
            return element['text'].get('content', '')
        return ''

    for prop in page['properties'].values():
        title = prop.get('title', [])
        if title:
            return ' '.join([
                _get_element_text(el) for el in title
            ])
    return ''


def fetch_user_data(user_id: str, sleeping: int = 0) -> str:
    logger = logging.getLogger(__name__)
    if user_id not in Users:
        logger.info('Fetching user data for %s', user_id)
        response = requests.get(f"https://api.notion.com/v1/users/{user_id}",
                                headers=get_headers(),
                                timeout=Timeout)

        if response.status_code != 200:
            raise E.AwordFetchError('fetch_user_data failed.', response)

        user_data = response.json()

        if sleeping:
            time.sleep(sleeping)

        Users[user_id] = user_data['name']

    return Users[user_id]


def fetch_page_authors(page: Dict, sleeping: int = 0) -> Tuple:
    created_by_id = page['created_by']['id']
    last_edited_by_id = (created_by_id if 'last_edited_by' not in page
                         else page['last_edited_by']['id'])

    logger = logging.getLogger(__name__)
    logger.info('Fetching page authors')
    created_by = fetch_user_data(created_by_id, sleeping)
    last_edited_by = (created_by if last_edited_by_id == created_by_id
                      else fetch_user_data(last_edited_by_id, sleeping))

    return created_by, last_edited_by, last_edited_by_id


def parse_page(title: str,
               created_by: str,
               last_edited_by: str,
               last_edited_by_id: str,
               url: str,
               max_break_level: int,
               timestamp: str,
               content: List):

    logger = logging.getLogger(__name__)
    logger.info('Parsing %s', title)

    def _text_from_block(block):
        out = []
        if block['type'] in block:
            for rich_text in block[block['type']].get('rich_text', []):
                out.append(rich_text.get('plain_text'))
        return ' '.join(out)

    segments = []
    current_heading_chain = []
    current_text = []
    current_heading_id = ''
    cutting_level = None
    for block in content:
        if block["object"] != "block":
            continue

        if block["type"].startswith("heading"):

            # Extract level from heading type (e.g. "heading_1" => 1)
            level = int(block["type"][-1])

            # If the level is low enough we break the segment
            if level <= max_break_level:
                text_so_far = '\n'.join(current_text).strip()
                if text_so_far:
                    block_last_edited_by_id = block["last_edited_by"]["id"]
                    block_last_edited_by = (last_edited_by
                                            if (block_last_edited_by_id ==
                                                last_edited_by_id)
                                            else
                                            fetch_user_data(block_last_edited_by_id))
                    segments.append(
                        Segment(body=text_so_far,
                                uri=url + (('#' + current_heading_id)
                                           if current_heading_id else ''),
                                # Extract heading texts from the stack
                                headings=[title] + [heading[1] for heading in
                                                    current_heading_chain],
                                created_by=created_by,
                                last_edited_by=block_last_edited_by,
                                last_edited_timestamp=timestamp))
                    current_text = []
                    current_heading_id = block['id'].replace('-', '')

                if current_heading_chain and level <= current_heading_chain[-1][0]:
                    # Pop headings from the stack until we reach the
                    # correct level
                    while (current_heading_chain and level <=
                           current_heading_chain[-1][0]):
                        current_heading_chain.pop()
                # Push the current heading onto the stack
                current_heading_chain.append((level, _text_from_block(block)))

            # A heading like deeper than level does not break segments, have it
            # at a single line.
            else:
                text = '- ' + _text_from_block(block)
                if text:
                    current_text.append('\n' + text.strip() + '\n')
        else:
            text = _text_from_block(block)
            if text:
                current_text.append(text)

    return segments

def process_page(page_id: str,
                 source_unit_cache: Cache,
                 categories: List[str] = '',
                 scope: str = '',
                 context: str = '',
                 language: str = '',
                 recurse_subpages: bool = False,
                 max_break_level: int = 2,
                 visited_pages: set = None,
                 sleeping: int = 0,
                 **_) -> [Segment]:

    short_page_id = page_id.replace('-', '')

    visited = visited_pages or set([])
    if short_page_id in visited:
        return []

    logger = logging.getLogger(__name__)

    try:
        page = fetch_page(page_id)
        if sleeping:
            time.sleep(sleeping)
        page_content = fetch_block_children(page_id, sleeping=sleeping)
    except Exception as e:
        logger.error('Failed tying to fetch %s:/n%s', page_id, str(e))
        return []

    last_edited_dt = T.timestamp_as_utc(page.get(
        'last_edited_time', page['created_time']))

    last_stored_edit_dt = source_unit_cache.get_last_edited_timestamp(SourceName, short_page_id)
    logger.debug('last_stored_edit_dt (%s, %s): %s',
                 SourceName,
                 short_page_id,
                 last_stored_edit_dt)

    if last_stored_edit_dt is None or last_edited_dt > last_stored_edit_dt:
        created_by, last_edited_by, last_edited_by_id = fetch_page_authors(page)
        try:
            segments = parse_page(extract_page_title(page),
                                  created_by=created_by,
                                  last_edited_by=last_edited_by,
                                  last_edited_by_id=last_edited_by_id,
                                  url=page['url'],
                                  max_break_level=max_break_level,
                                  timestamp=last_edited_dt,
                                  content=page_content)

            if segments:
                source_unit_cache.add_or_update(source=SourceName,
                                                source_unit_id=short_page_id,
                                                uri=page['url'],
                                                categories=categories,
                                                scope=scope,
                                                context=context,
                                                language=language,
                                                created_by=created_by,
                                                last_edited_by=last_edited_by,
                                                last_edited_timestamp=last_edited_dt,
                                                segments=segments)
        except:
            logger.error('Failed processing page %s', page_id)

    if recurse_subpages:
        visited.add(short_page_id)
        for block in page_content:
            if block['type'] == 'child_page':
                segments += process_page(page_id=block['id'],
                                         source_unit_cache=source_unit_cache,
                                         categories=categories,
                                         scope=scope,
                                         language=language,
                                         recurse_subpages=recurse_subpages,
                                         max_break_level=max_break_level,
                                         visited_pages=visited,
                                         sleeping=sleeping)
    return segments


def add_to_cache(awd: Awd,
                 sleeping: int = 0.5):
    ensure_api_key(awd)
    sources = awd.get_single_source_config(SourceName, {})

    pages_args = sources.get('pages', [])

    for database in sources.get('databases', []):
        pages_args.extend([{'page_id': page_id, **database}
                           for page_id in
                           fetch_all_pages_in_database(database['database_id'])])


    source_unit_cache = awd.get_source_unit_cache()
    segments = []
    for args in pages_args:
        segments += process_page(page_id=args['page_id'],
                                 source_unit_cache=source_unit_cache,
                                 categories=args.get('categories', []),
                                 scope=args['scope'],
                                 recurse_subpages=args.get('recursive', True),
                                 max_break_level=args.get('max_break_level', 2),
                                 sleeping=sleeping)
    return segments


def add_args(parser):
    parser.add_argument('--add-to-cache',
                        action='store_true',
                        help='Add notion documents to the cache')


def main(awd, args):
    if args['add_to_cache']:
        add_to_cache(awd)
