# -*- coding: utf-8 -*-

import datetime
import pickle
import json
import sqlite3
from sqlite3 import Error
from typing import Optional, Dict, Any, List
from datetime import datetime

import aword.tools as T
from aword.vdbfields import VectorDbFields
from aword.segment import Segment
from aword.chunk import Chunk


DbConnection = None

def get_connection(in_memory=False):
    global DbConnection
    if DbConnection is None:
        try:
            DbConnection = sqlite3.connect(':memory:' if in_memory
                                           else T.get_config('edge')['sqlite_db_file'])
            DbConnection.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(e)
    return DbConnection


def close_connection():
    global DbConnection
    if DbConnection is not None:
        DbConnection.close()
        DbConnection = None


def timestamp_str(ts):
    return T.timestamp_as_utc(ts).strftime('%Y-%m-%d %H:%M:%S.%f') if ts else ''


def timestamps_to_datetimes(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is not None:
        out = dict(row)
        out['last_edited_timestamp'] = T.timestamp_as_utc(
            out['last_edited_timestamp'] + '+00:00')
        if out['last_embedded_timestamp']:
            out['last_embedded_timestamp'] = T.timestamp_as_utc(
                out['last_embedded_timestamp'] + '+00:00')
        else:
            out['last_embedded_timestamp'] = None
        out['metadata'] = json.loads(out['metadata']) or {}
        out['segments'] = pickle.loads(out['segments']) or []
        return out
    return None


class SourceUnit:
    def __init__(self, in_memory=False):
        self.conn = get_connection(in_memory)
        self.create_table()

    def create_table(self):
        try:
            self.conn.execute(f"""
                CREATE TABLE source_unit (
                    {VectorDbFields.SOURCE_UNIT_ID.value} TEXT PRIMARY KEY,
                    {VectorDbFields.SOURCE.value} TEXT,
                    uri TEXT,
                    created_by TEXT,
                    last_edited_by TEXT,
                    last_edited_timestamp TIMESTAMP,
                    last_embedded_timestamp TIMESTAMP,
                    {VectorDbFields.CATEGORY.value} TEXT,
                    {VectorDbFields.SCOPE.value} TEXT,
                    summary TEXT,
                    segments BLOB,
                    metadata TEXT
                )
            """)
        except Error as e:
            print(e)

    def add(self,
            uri: str,
            created_by: str,
            last_edited_by: str,
            last_edited_timestamp: datetime,
            summary: str,
            segments: List[Segment],
            metadata: Dict = None,
            last_embedded_timestamp: datetime = None,
            **vector_db_fields):

        metadata_str = json.dumps(metadata, sort_keys=True)

        query = """
            INSERT OR REPLACE INTO source_unit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value],
                                  vector_db_fields[VectorDbFields.SOURCE.value],
                                  T.validate_uri(uri),
                                  created_by,
                                  last_edited_by,
                                  timestamp_str(last_edited_timestamp),
                                  timestamp_str(last_embedded_timestamp),
                                  vector_db_fields.get(VectorDbFields.CATEGORY.value, ''),
                                  vector_db_fields.get(VectorDbFields.SCOPE.value, ''),
                                  summary,
                                  pickle.dumps(segments),
                                  metadata_str))
        self.conn.commit()
        return vector_db_fields[VectorDbFields.SOURCE_UNIT_ID.value]

    def get(self, source_unit_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE source_unit_id=?",
                       (source_unit_id,))
        out = timestamps_to_datetimes(cursor.fetchone())
        return out


    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE uri=?", (uri,))
        return timestamps_to_datetimes(cursor.fetchone())

    def get_unembedded(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM source_unit
            WHERE last_embedded_timestamp IS NULL OR last_embedded_timestamp < last_edited_timestamp
        """)

        rows = cursor.fetchall()
        return [timestamps_to_datetimes(row) for row in rows]
