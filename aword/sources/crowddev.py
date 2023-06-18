# -*- coding: utf-8 -*-

import os
from datetime import datetime
from typing import Dict, List

import requests

import aword.tools as T
from aword.payload import Payload, FactType
from aword.embed import embed_source_unit
from aword.sources.state import State

SourceName = 'crowddev'


def get_activities(from_timestamp: datetime, offset: int = 0):
    T.load_environment()
    tenant_id = os.environ.get('CROWDDEV_TENANT_ID')
    api_key = os.environ.get('CROWDDEV_API_KEY')
    if not tenant_id and api_key:
        return []

    url = f"https://app.crowd.dev/api/tenant/{tenant_id}/activity/query"

    payload = {
        "limit": 200,
        "offset": offset,
        "filter": {
            "timestamp": {"gte": "2022-09-01"},
        },
        "orderBy": "timestamp_DESC",
    }

    # TODO: is this correct? Do we want all the activities from the
    # very beginning the first time we start ingesting?
    if from_timestamp is not None:
        payload['filter']['createdAt'] = {"gte": from_timestamp.strftime("%Y-%m-%d")}

    headers = {
        "accept": "application/json",
        "content-type": "application/json",
        "authorization": f"Bearer {api_key}",
    }

    response = requests.post(url, json=payload, headers=headers, timeout=15)
    return response.json()["rows"]


def ingest_activities(activities: List[Dict]):
    payloads = []

    for activity in activities:
        # TODO: is it fine to have the platform as part of the metadata?
        metadata = activity['attributes']
        metadata['platform'] = activity['platform']
        payloads.append(Payload(activity['body'],
                                source_unit_id=activity['id'],
                                uri=activity['url'],
                                headings=[],
                                created_by=activity['member']['displayName'],
                                source=SourceName,
                                fact_type=FactType.historical,
                                timestamp=activity['createdAt'],
                                metadata=metadata))

    embed_source_unit(payloads)


def ingest():
    state = State()

    last_seen_dt = state.get_last_seen(SourceName, SourceName)
    state.update_last_seen(SourceName, SourceName)

    offset = 0
    activities = get_activities(last_seen_dt, offset=offset)
    while len(activities) > 0:
        ingest_activities(activities)
        offset += 100
        activities = get_activities(last_seen_dt, offset)


if __name__ == '__main__':
    ingest()
