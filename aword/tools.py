# -*- coding: utf-8 -*-

import os
import datetime
from typing import Dict, List, Union

import urllib
import urllib.request


def timestamp_as_utc(timestamp: Union[datetime.datetime, str] = None) -> datetime.datetime:
    if timestamp:
        if not isinstance(timestamp, (str, datetime.datetime)):
            raise ValueError("timestamp must be a string in ISO 8601 "
                             "format or a datetime object.")

        if isinstance(timestamp, str):
            timestamp = datetime.datetime.fromisoformat(timestamp)

        if timestamp.tzinfo is None or timestamp.tzinfo.utcoffset(timestamp) is None:
            timestamp = timestamp.replace(
                tzinfo=datetime.datetime.now().astimezone().tzinfo)
        return timestamp.astimezone(datetime.timezone.utc)

    return datetime.datetime.now(datetime.timezone.utc)


def validate_uri(uri, raise_if_invalid=True):
    if uri:
        # Make sure URI is valid
        if urllib.parse.urlparse(uri).scheme == "":
            if raise_if_invalid:
                raise ValueError(f"Invalid URI: {uri}")
            return None
    return uri


def file_to_uri(file_path):
    return urllib.parse.urljoin('file:',
                                urllib.request.pathname2url(os.path.abspath(file_path)))


def uri_to_file_path(uri):
    path = urllib.parse.urlparse(uri).path
    return os.path.abspath(urllib.request.url2pathname(path))


def generate_anchor(headings: List[str] = None,
                    prev_anchors: Dict[str, int] = None) -> str:
    """Where headings is a list of headings, from highest to lowest level.

    When an anchor is repeated in a file we append a number to the
    end. This is apparently what github does.
    """
    prev_anchors_copy = prev_anchors.copy()
    anchor_number = ''
    if headings:
        anchor = urllib.parse.quote(headings[-1])
        if anchor in prev_anchors:
            # The first repeated anchor has a -2 appended.
            prev_anchors_copy[anchor] += 1
            anchor_number = '-' + str(prev_anchors[anchor])
        else:
            prev_anchors_copy[anchor] = 1

    return (('#' + anchor + anchor_number) if anchor else ''), prev_anchors_copy
