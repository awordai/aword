# -*- coding: utf-8 -*-

import json
import os
from datetime import datetime
from pytz import utc

import aword.tools as T


class State:
    def __init__(self, db_path=None):
        C = T.get_config('state')
        self.db_path = db_path or C['last_seen_db']

        if os.path.exists(self.db_path):
            with open(self.db_path, 'r', encoding='utf-8') as db_file:
                self.db = json.load(db_file)
                # Convert date strings back into datetime objects
                for subsources in self.db.values():
                    for subsource, timestamp in subsources.items():
                        subsources[subsource] = datetime.fromisoformat(timestamp)
        else:
            self.db = {}

    def get_last_seen(self, source, subsource):
        return self.db.get(source, {}).get(subsource, None)

    def update_last_seen(self, source, subsource):
        if source not in self.db:
            self.db[source] = {}
        # Save the current datetime in UTC
        self.db[source][subsource] = datetime.now(utc)

        with open(self.db_path, 'w', encoding='utf-8') as db_file:
            db_copy = {source: {subsource: timestamp.isoformat()
                                for subsource, timestamp in subsources.items()}
                       for source, subsources in self.db.items()}
            json.dump(db_copy, db_file)
