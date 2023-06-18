# -*- coding: utf-8 -*-

import os
import sys
import json
import datetime
import configparser
from typing import Dict, List, Union

import urllib
import urllib.request

from dotenv import load_dotenv


SourceConfig = {}
Config = {}


def get_source_config(source_name: str,
                      default: Union[Dict, List] = None) -> Union[Dict, List]:
    global SourceConfig

    if not SourceConfig:
        # Define potential locations for `sources.json`
        config_locations = [
            os.path.join(os.path.dirname(__file__), '..', 'sources.json'),  # Repository
            os.environ.get('AWORD_SOURCES_CONFIG'),  # Environment variable
            os.path.expanduser('~/.aword/sources.json')  # User's home directory
        ]

        # Find the first existing configuration file in the list
        config_path = next((path for path in config_locations
                            if path and os.path.isfile(path)), None)

        if config_path is None:
            return default

        with open(config_path, 'r', encoding='utf-8') as f:
            SourceConfig = json.load(f)

    return SourceConfig.get(source_name, default if default is not None else {})


def get_config(section: str) -> Dict:
    if not Config:
        config = configparser.ConfigParser()

        # Define potential locations for `config.ini`
        config_locations = [
            os.path.join(os.path.dirname(__file__), '..', 'config.ini'),  # Repository
            os.environ.get('AWORD_CONFIG'),  # Environment variable
            os.path.expanduser('~/.aword/config.ini')  # User's home directory
        ]

        # Find the first existing configuration file in the list
        config_path = next((path for path in config_locations
                            if path and os.path.isfile(path)), None)

        if config_path is None:
            raise FileNotFoundError('No configuration file found.')

        config.read(config_path)

        for s in config.sections():
            Config[s] = dict(config[s])

            # Attempt to convert numeric values to integers or floats
            for key, value in Config[s].items():
                try:
                    if '.' in value:
                        Config[s][key] = float(value)
                    else:
                        Config[s][key] = int(value)
                except ValueError:
                    pass

    return Config.get(section, {})


def load_environment():
    load_dotenv('.env.test' if 'pytest' in sys.argv[0] else '.env')


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
    if uri is not None:
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
