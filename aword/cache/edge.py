# -*- coding: utf-8 -*-

import datetime
import pickle
import uuid
import json
import sqlite3
from sqlite3 import Error
from typing import Optional, Dict, Any, List, Union
from datetime import datetime

import aword.tools as T


def create_connection():
    try:
        C = T.get_config('edge')
        conn = sqlite3.connect(C['sqlite_db_file'])
        conn.row_factory = sqlite3.Row
        return conn
    except Error as e:
        print(e)
        return None


def timestamp_str(ts):
    return T.timestamp_as_utc(ts).strftime('%Y-%m-%d %H:%M:%S.%f')


def timestamps_to_datetimes(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    if row is not None:
        result = dict(row)
        result['last_edited_timestamp'] = T.timestamp_as_utc(
            result['last_edited_timestamp'] + '+00:00')
        result['last_embedded_timestamp'] = T.timestamp_as_utc(
            result['last_embedded_timestamp'] + '+00:00')
        return result
    return None




class SourceUnitDB:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.create_table()

    def create_table(self):
        try:
            self.conn.execute("""
                CREATE TABLE source_unit (
                    source_unit_id TEXT PRIMARY KEY,
                    source TEXT,
                    uri TEXT,
                    created_by TEXT,
                    last_edited_by TEXT,
                    last_edited_timestamp TIMESTAMP,
                    last_embedded_timestamp TIMESTAMP,
                    category TEXT,
                    scope TEXT,
                    summary TEXT,
                    body TEXT,
                    metadata TEXT
                )
            """)
        except Error as e:
            print(e)

    def add_source_unit(self,
                        source_unit_id: str,
                        source: str,
                        uri: str,
                        created_by: str,
                        last_edited_by: str,
                        last_edited_timestamp: datetime,
                        last_embedded_timestamp: datetime,
                        fact_type: str,
                        scope: str,
                        summary: str,
                        body: str,
                        metadata: Dict):

        query = """
            INSERT INTO source_unit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (source_unit_id,
                                  source,
                                  uri,
                                  created_by,
                                  last_edited_by,
                                  timestamp_str(last_edited_timestamp),
                                  timestamp_str(last_embedded_timestamp),
                                  fact_type,
                                  scope,
                                  summary,
                                  body,
                                  metadata))
        self.conn.commit()

        return source_unit_id

    def get(self, source_unit_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE source_unit_id=?",
                       (source_unit_id,))
        return timestamps_to_datetimes(cursor.fetchone())

    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE uri=?", (uri,))
        return timestamps_to_datetimes(cursor.fetchone())


class Section:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.create_table()

    def create_table(self):
        try:
            self.conn.execute("""
                CREATE TABLE section (
                    section_id TEXT PRIMARY KEY,
                    source_unit_id TEXT,
                    uri TEXT,
                    created_by TEXT,
                    last_edited_by TEXT,
                    last_edited_timestamp TIMESTAMP,
                    last_embedded_timestamp TIMESTAMP,
                    body TEXT,
                    headings TEXT,
                    metadata TEXT
                )
            """)
        except sqlite3.Error as e:
            print(e)

    def add_section(self,
                    source_unit_id: str,
                    uri: str,
                    created_by: str,
                    last_edited_by: str,
                    last_edited_timestamp: datetime,
                    last_embedded_timestamp: datetime,
                    body: str,
                    headings: List[str],
                    metadata: Dict[str, Any]):

        headings_str = json.dumps(headings, sort_keys=True)
        metadata_str = json.dumps(metadata, sort_keys=True)

        # Generate a UUID based on the source_unit_id, body, headings, and metadata
        name = source_unit_id + body + headings_str + metadata_str
        section_id = str(uuid.uuid5(uuid.NAMESPACE_URL, name))

        query = """
            INSERT INTO section
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (section_id,
                                  source_unit_id,
                                  uri,
                                  created_by,
                                  last_edited_by,
                                  timestamp_str(last_edited_timestamp),
                                  timestamp_str(last_embedded_timestamp),
                                  body,
                                  headings_str,
                                  metadata_str))
        self.conn.commit()
        return section_id

    def get(self, section_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM section WHERE section_id=?",
                       (section_id,))
        out = timestamps_to_datetimes(cursor.fetchone())
        out['headings'] = json.loads(out['headings']) or []
        out['metadata'] = json.loads(out['metadata']) or {}
        return out

    def get_by_uri(self, uri: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM section WHERE uri=?", (uri,))
        out = timestamps_to_datetimes(cursor.fetchone())
        out['headings'] = json.loads(out['headings']) or []
        out['metadata'] = json.loads(out['metadata']) or {}
        return out


class ChunkDB:
    def __init__(self, conn: sqlite3.Connection):
        self.conn = conn
        self.create_table()

    def create_table(self):
        try:
            self.conn.execute("""
                CREATE TABLE chunk (
                    chunk_id TEXT PRIMARY KEY,
                    section_id TEXT,
                    body TEXT,
                    vector_embedding BLOB
                )
            """)
        except Error as e:
            print(e)

    def add_chunk(self,
                  chunk_id: str,
                  section_id: str,
                  body: str,
                  vector_embedding: List[float]):
        query = """
            INSERT INTO chunk
            VALUES (?, ?, ?, ?)
        """
        # Convert list of floats to a binary BLOB
        vector_embedding_blob = pickle.dumps(vector_embedding)
        self.conn.execute(query, (chunk_id,
                                  section_id,
                                  body,
                                  vector_embedding_blob))
        self.conn.commit()


    def get(self, chunk_id: str) -> Optional[Dict[str, Union[str, List[float]]]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM chunk WHERE chunk_id=?",
                       (chunk_id,))
        result = cursor.fetchone()
        if result:
            # Convert result to a dictionary
            result = dict(zip([desc[0] for desc in cursor.description], result))
            # Convert BLOB back to list of floats
            result['vector_embedding'] = pickle.loads(result['vector_embedding'])
        return result
