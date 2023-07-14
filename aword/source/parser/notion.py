# -*- coding: utf-8 -*-

import os
import time
from typing import Dict, List, Callable, Tuple

import requests

import aword.tools as T
from aword.segment import Segment
from aword.vector.fields import VectorDbFields


SourceName = 'notion'
Timeout = 30
Users = {}

Testing = False


def get_headers():
    T.load_environment()
    api_key = os.environ.get('NOTION_API_KEY', '')
    if not api_key:
        raise RuntimeError('No NOTION_API_KEY found as an environment variable')

    return {
        "Authorization": f"Bearer {api_key}",
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
            raise RuntimeError(f"Request failed with status code {response.status_code}")

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
    print('Fetching block/page content for', block_id)
    url = f"https://api.notion.com/v1/blocks/{block_id}/children"
    response = requests.get(url, headers=get_headers(), timeout=Timeout)

    if response.status_code != 200:
        raise RuntimeError(f"Request failed with status code {response.status_code}")

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
    print('Fetching page', page_id)
    url = f"https://api.notion.com/v1/pages/{page_id}"
    response = requests.get(url, headers=get_headers(), timeout=Timeout)

    if response.status_code != 200:
        raise RuntimeError(f"Request failed with status code {response.status_code}")

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
    if user_id not in Users:
        print('Fetching user data for', user_id)
        response = requests.get(f"https://api.notion.com/v1/users/{user_id}",
                                headers=get_headers(),
                                timeout=Timeout)

        if response.status_code != 200:
            raise RuntimeError(f"Request failed with status code {response.status_code}")
        user_data = response.json()

        if sleeping:
            time.sleep(sleeping)

        Users[user_id] = user_data['name']

    return Users[user_id]


def fetch_page_authors(page: Dict, sleeping: int = 0) -> Tuple:
    created_by_id = page['created_by']['id']
    last_edited_by_id = (created_by_id if 'last_edited_by' not in page
                         else page['last_edited_by']['id'])

    print('Fetching page authors')
    created_by = fetch_user_data(created_by_id, sleeping)
    last_edited_by = (created_by if last_edited_by_id == created_by_id
                      else fetch_user_data(last_edited_by_id, sleeping))

    return created_by, last_edited_by, last_edited_by_id


def ingest_page(title: str,
                created_by: str,
                last_edited_by: str,
                last_edited_by_id: str,
                url: str,
                timestamp: str,
                content: List,
                category: str,
                scope: str):

    print('Ingesting', title)

    source_unit_id = url

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
    for block in content:
        if block["object"] == "block":
            if block["type"].startswith("heading"):

                text_so_far = '\n\n'.join(current_text).strip()
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

                # Extract level from heading type (e.g. "heading_1" => 1)
                level = int(block["type"][-1])
                if current_heading_chain and level <= current_heading_chain[-1][0]:
                    # Pop headings from the stack until we reach the
                    # correct level
                    while (current_heading_chain and level <=
                           current_heading_chain[-1][0]):
                        current_heading_chain.pop()
                # Push the current heading onto the stack
                current_heading_chain.append((level, _text_from_block(block)))
            else:
                text = _text_from_block(block)
                if text:
                    current_text.append(text)

    return embed_source_unit(segments, source_unit_id=source_unit_id)


def process_page(page_id: str,
                 ingesting_function: Callable,
                 cache: Cache,
                 category: str = '',
                 scope: str = '',
                 recurse_subpages: bool = False,
                 visited_pages: set = None,
                 sleeping: int = 0):

    short_page_id = page_id.replace('-', '')

    visited = visited_pages or set([])
    if short_page_id in visited:
        return

    try:
        page = fetch_page(page_id)
        if sleeping:
            time.sleep(sleeping)
        page_content = fetch_block_children(page_id, sleeping=sleeping)
    except:
        print('Failed tying to fetch', page_id)
        if Testing:
            raise

    last_edited_tm = T.timestamp_as_utc(page.get(
        'last_edited_time', page['created_time']))

    last_seen_dt = cache.get_last_seen(SourceName, short_page_id)

    if (last_seen_dt is None) or (last_edited_tm > last_seen_dt):
        created_by, last_edited_by, last_edited_by_id = fetch_page_authors(page)
        try:
            ingesting_function(extract_page_title(page),
                               created_by=created_by,
                               last_edited_by=last_edited_by,
                               last_edited_by_id=last_edited_by_id,
                               url=page['url'],
                               timestamp=last_edited_tm,
                               content=page_content,
                               category=category,
                               scope=scope)
            cache.update_last_seen(SourceName, short_page_id)
        except:
            print('Failed ingesting page', page_id)
            if Testing:
                raise


    if recurse_subpages:
        visited.add(short_page_id)
        for block in page_content:
            if block['type'] == 'child_page':
                process_page(block['id'],
                             ingesting_function,
                             state=state,
                             category=category,
                             scope=scope,
                             recurse_subpages=True,
                             visited_pages=visited,
                             sleeping=sleeping)


def ingest(cache: Cache, sleeping: int = 0.5):
    sources = T.get_source_config(SourceName)

    page_id_categories_scopes = []
    for database in sources.get('databases', []):
        page_id_categories_scopes.extend(
            [(page_id,
              database.get('category', ''),
              database.get('scope', '')) for page_id in
             fetch_all_pages_in_database(database['id'])]
        )


    for page in sources.get('pages', []):
        page_id_categories_scopes.append((page['id'],
                                          page.get('category', ''),
                                          page.get('scope', '')))

    time.sleep(sleeping)
    for (page_id, category, scope) in page_id_categories_scopes:
        process_page(page_id,
                     ingesting_function=ingest_page,
                     cache=cache,
                     category=category,
                     scope=scope,
                     recurse_subpages=True,
                     sleeping=sleeping)


if __name__ == "__main__":
    from aword.cache.edge import SourceUnit
    ingest(SourceUnit())