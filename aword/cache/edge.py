# -*- coding: utf-8 -*-

import datetime
import pickle
import json
from itertools import groupby
import uuid

import sqlite3
from sqlite3 import Error
from typing import Optional, Dict, Any, List
from datetime import datetime

from pytz import utc

import aword.tools as T
from aword.vdbfields import VectorDbFields
from aword.segment import Segment
from aword.chunk import Chunk
from aword.cache import Cache


DbConnection = None

Source = VectorDbFields.SOURCE.value
Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
Category = VectorDbFields.CATEGORY.value
Scope = VectorDbFields.SCOPE.value


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
            out['last_edited_timestamp'] + ('+00:00' if '+' not in out['last_edited_timestamp']
                                            else ''))
        if out['last_embedded_timestamp']:
            out['last_embedded_timestamp'] = T.timestamp_as_utc(
                out['last_embedded_timestamp'] + (
                    '+00:00' if '+' not in out['last_embedded_timestamp']
                    else ''))
        else:
            out['last_embedded_timestamp'] = None
        out['metadata'] = json.loads(out['metadata']) or {}
        out['segments'] = pickle.loads(out['segments']) or []
        return out
    return None


class SourceUnitDB(Cache):
    def __init__(self, in_memory=False):
        self.conn = get_connection(in_memory)
        self.create_table()
        self.create_history_table()
        self.in_memory = in_memory

    def create_table(self):
        try:
            self.conn.execute(f"""
                CREATE TABLE source_unit (
                    {Source} TEXT,
                    {Source_unit_id} TEXT,
                    uri TEXT,
                    created_by TEXT,
                    last_edited_by TEXT,
                    last_edited_timestamp TIMESTAMP,
                    last_embedded_timestamp TIMESTAMP,
                    {Category} TEXT,
                    {Scope} TEXT,
                    summary TEXT,
                    segments BLOB,
                    metadata TEXT,
                    PRIMARY KEY({Source_unit_id}, {Source})
                )
            """)
        except Error as e:
            print(e)

    def reset_tables(self, only_in_memory=True):
        if not self.in_memory and only_in_memory:
            print('Refusing to drop persistent tables without `only_in_memory` argument')
            return
        try:
            self.conn.execute("DROP TABLE IF EXISTS source_unit")
            self.conn.execute("DROP TABLE IF EXISTS source_unit_history")
            self.create_table()
            self.create_history_table()
        except Error as e:
            print(e)

    def create_history_table(self):
        try:
            self.conn.execute(f"""
                CREATE TABLE source_unit_history (
                    {Source} TEXT,
                    {Source_unit_id} TEXT,
                    uri TEXT,
                    created_by TEXT,
                    last_edited_by TEXT,
                    last_edited_timestamp TIMESTAMP,
                    last_embedded_timestamp TIMESTAMP,
                    {Category} TEXT,
                    {Scope} TEXT,
                    summary TEXT,
                    segments BLOB,
                    metadata TEXT,
                    deleted TIMESTAMP,
                    PRIMARY KEY({Source_unit_id}, {Source}, deleted)
                )
            """)
        except Error as e:
            print(e)

    def delete(self, source: str, source_unit_id: str):
        existing_record = self.get(source, source_unit_id)
        if existing_record:
            query = """
                INSERT INTO source_unit_history
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(query, (
                existing_record[Source],
                existing_record[Source_unit_id],
                existing_record['uri'],
                existing_record['created_by'],
                existing_record['last_edited_by'],
                existing_record['last_edited_timestamp'],
                existing_record['last_embedded_timestamp'],
                existing_record[Category],
                existing_record[Scope],
                existing_record['summary'],
                pickle.dumps(existing_record['segments']),
                json.dumps(existing_record['metadata'], sort_keys=True),
                timestamp_str(datetime.now(utc)),
            ))
            self.conn.execute("DELETE FROM source_unit WHERE "
                              f"{Source} = ? AND "
                              f"{Source_unit_id} = ?",
                              (source_unit_id, source))
            self.conn.commit()

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

        existing_record = self.get(vector_db_fields[Source],
                                   vector_db_fields[Source_unit_id])
        if existing_record:
            self.delete(vector_db_fields[Source],
                        vector_db_fields[Source_unit_id])

        query = """
            INSERT OR REPLACE INTO source_unit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (vector_db_fields[Source],
                                  vector_db_fields[Source_unit_id],
                                  T.validate_uri(uri),
                                  created_by,
                                  last_edited_by,
                                  timestamp_str(last_edited_timestamp),
                                  timestamp_str(last_embedded_timestamp),
                                  vector_db_fields.get(Category, ''),
                                  vector_db_fields.get(Scope, ''),
                                  summary,
                                  pickle.dumps(segments),
                                  json.dumps(metadata, sort_keys=True)))
        self.conn.commit()
        return vector_db_fields[Source_unit_id]

    def get(self, source: str, source_unit_id: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE "
                       f"{Source}=? AND "
                       f"{Source_unit_id}=?",
                       (source, source_unit_id))
        out = timestamps_to_datetimes(cursor.fetchone())
        return out

    def get_by_uri(self, source: str, uri: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE uri=? AND "
                       f"{Source}=?", (uri, source))
        return timestamps_to_datetimes(cursor.fetchone())

    def get_unembedded(self) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM source_unit
            WHERE last_embedded_timestamp IS NULL OR last_embedded_timestamp < last_edited_timestamp
        """)

        rows = cursor.fetchall()
        return [timestamps_to_datetimes(row) for row in rows]

    def get_last_edited(self, source: str, source_unit_id: str) -> Optional[datetime]:
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT last_edited_timestamp
            FROM source_unit
            WHERE source_unit_id = ? AND source = ?
            """, (source_unit_id, source))

        row = cursor.fetchone()
        if row is not None:
            return T.timestamp_as_utc(row['last_edited_timestamp'] + '+00:00')
        return None

    def get_history(self, source: str, source_unit_id: str) -> List[Dict[str, Any]]:
        """Returns the history of a source unit.

        Args:
            source: The source of the source unit.
            source_unit_id: The ID of the source unit.

        Returns:
            A list of dictionaries representing the history of the source unit.
        """
        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM source_unit_history
            WHERE source = ? AND source_unit_id = ?
            ORDER BY last_edited_timestamp DESC
        """, (source, source_unit_id))

        rows = cursor.fetchall()

        # Convert rows to dictionaries and convert timestamps to datetimes
        return [timestamps_to_datetimes(dict(row)) for row in rows]

    def get_state_at_date(self, date: datetime) -> List[Dict[str, Any]]:
        date_str = timestamp_str(date)

        cursor = self.conn.cursor()
        cursor.execute("""
            SELECT * FROM source_unit
            WHERE last_edited_timestamp <= ?
        """, (date_str,))
        current_records = cursor.fetchall()

        cursor.execute("""
            SELECT * FROM source_unit_history
            WHERE last_edited_timestamp <= ? AND deleted > ?
            ORDER BY source_unit_id, source, deleted DESC
        """, (date_str, date_str))
        historical_records = cursor.fetchall()

        # Convert records from sqlite3.Row to dictionary
        current_records = [timestamps_to_datetimes(row) for row in current_records]
        historical_records = [timestamps_to_datetimes(row) for row in historical_records]

        # Group by source_unit_id and source, taking only the first (latest) record for each group
        grouped_records = []
        for _, group in groupby(historical_records,
                                key=lambda row: (row[Source_unit_id],
                                                 row[Source])):
            grouped_records.append(next(group))

        # Step 3
        all_records = {(row[Source_unit_id],
                        row[Source]): row for row in current_records}
        all_records.update({(row[Source_unit_id],
                             row[Source]): row
                            for row in grouped_records})

        return list(all_records.values())


class ChunkDB:
    def __init__(self, model_name, in_memory=False):
        self.conn = get_connection(in_memory)
        self.table_name = 'chunk_' + model_name.replace('-', '_')

        self.create_table()
        self.in_memory = in_memory

    def create_table(self):
        try:
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {self.table_name} (
                    chunk_id TEXT,
                    {Source} TEXT,
                    {Source_unit_id} TEXT,
                    text TEXT,
                    vector BLOB,
                    vector_db_id TEXT,
                    PRIMARY KEY(chunk_id, {Source}, {Source_unit_id}),
                    FOREIGN KEY({Source}, {Source_unit_id}) REFERENCES source_unit({Source}, {Source_unit_id})
                )
            """)
            self.conn.execute(f"""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_{self.table_name}_chunk_id
                ON {self.table_name} (chunk_id)
            """)
        except sqlite3.Error as e:
            print(e)

    def add(self, source: str, source_unit_id: str, chunks: List[Chunk]):
        try:
            self.conn.executemany(f"""
                INSERT OR REPLACE INTO {self.table_name}
                VALUES (?, ?, ?, ?, ?, ?)
            """, [(str(uuid.uuid5(uuid.NAMESPACE_URL, chunk.text)),
                   source,
                   source_unit_id,
                   chunk.text,
                   json.dumps(chunk.vector),
                   chunk.vector_db_id)
                  for chunk in chunks])
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)

    def get(self, chunk_id: str) -> Optional[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE chunk_id=?", (chunk_id,))
        row = cursor.fetchone()
        return Chunk(row['text'], json.loads(row['vector']), row['vector_db_id']) if row else None

    def get_by_source_unit(self, source: str, source_unit_id: str) -> List[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE source=? AND source_unit_id=?",
                       (source, source_unit_id))
        rows = cursor.fetchall()
        return [Chunk(row['text'], json.loads(row['vector']), row['vector_db_id']) for row in rows]

    def reset_vector_db_id_by_source_unit(self, source: str, source_unit_id: str):
        try:
            self.conn.execute(f"""
                UPDATE {self.table_name}
                SET vector_db_id = NULL
                WHERE source = ? AND source_unit_id = ?
            """, (source, source_unit_id))
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)
