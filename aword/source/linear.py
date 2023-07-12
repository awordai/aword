# -*- coding: utf-8 -*-


import os
from datetime import datetime
from typing import Dict, List

import requests

import aword.tools as T
from aword.payload import Segment
from aword.embed import embed_source_unit
from aword.sources.state import State

SourceName = 'linear'


def get_issues(from_timestamp: datetime,
               cursor: str = None,
               page_size: int = 50):
    T.load_environment()

    # TODO Should we add the title to the parent, and use it as a heading?
    query = """
    query GetIssues($cursor: String, $pageSize: Int, $updatedAfter: DateTime) {
        issues(first: $pageSize, after: $cursor, filter: {updatedAt:{ gt: $updatedAfter}}) {
            nodes {
              id
              title
              description
              state {
                name
              }
              estimate
              priorityLabel
              creator {
                name
              }
              assignee {
                name
              }
              labels {
                nodes {
                  name
                }
              }
              cycle {
                name
              }
              createdAt
              startedAt
              completedAt
              canceledAt
              parent {
                id
              }
              updatedAt
              url
            }
            pageInfo {
                endCursor
                hasNextPage
            }
        }
    }
    """

    api_key = os.environ.get("LINEAR_API_KEY", '')
    if not api_key:
        return []

    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    variables = {
        "pageSize": page_size,
        # TODO: this will include the timezone, is it going to work?
        "updatedAfter": from_timestamp.isoformat(),
    }

    # TODO: I assume that when cursor is None we should not add it?
    if cursor is not None:
        variables['cursor'] = cursor

    response = requests.post(
        "https://api.linear.app/graphql",
        json={"query": query, "variables": variables},
        headers=headers,
        timeout=15
    )
    data = response.json()

    if response.status_code == 200:
        issues = data["data"]["issues"]["nodes"]
        end_cursor = data["data"]["issues"]["pageInfo"]["endCursor"]
        has_next_page = data["data"]["issues"]["pageInfo"]["hasNextPage"]

        ingest_issues(issues)

        if has_next_page:
            return issues + get_issues(
                from_timestamp, cursor=end_cursor, page_size=page_size
            )
        return issues
    raise RuntimeError(f'Error fetching issues: {data.get("errors", "")}')


def ingest_issues(issues: List[Dict]):
    segments = []

    for issue in issues:
        labels = ", ".join([label["name"] for label in issue["labels"]["nodes"]])
        creator = issue["creator"]["name"] if issue["creator"] else "N/A"
        assignee = issue["assignee"]["name"] if issue["assignee"] else "N/A"
        parent_id = issue["parent"]["id"] if issue["parent"] else "N/A"

        metadata = {"status": issue["state"]["name"],
                    "estimate": issue["estimate"],
                    "priority": issue["priorityLabel"],
                    "labels": labels,
                    "cycle": issue["cycle"]["name"] if issue["cycle"] else "N/A",
                    "created": issue["createdAt"],
                    "assignee": assignee,
                    "parent": parent_id,}

        segments.append(Segment(issue['description'] if issue['description'] else '',
                                source_unit_id=issue['id'],
                                uri=issue['url'],
                                headings=[],
                                created_by=creator,
                                source=SourceName,
                                category='historical',
                                scope='confidential',
                                last_edited_timestamp=issue["updatedAt"],
                                metadata=metadata))

    embed_source_unit(segments)


def ingest():
    state = State()

    last_seen_dt = state.get_last_seen(SourceName, SourceName)
    state.update_last_seen(SourceName, SourceName)

    get_issues(last_seen_dt)


if __name__ == "__main__":
    ingest()
