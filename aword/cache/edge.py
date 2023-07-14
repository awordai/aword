# -*- coding: utf-8 -*-

import pickle
import json
from itertools import groupby
import uuid

import sqlite3
from sqlite3 import Error
from datetime import datetime
from typing import Optional, Dict, Any, List

from pytz import utc

import aword.tools as T
from aword.vector.fields import VectorDbFields
from aword.segment import Segment
from aword.chunk import Chunk
from aword.cache.cache import Cache


DbConnection = None

Source = VectorDbFields.SOURCE.value
Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
Category = VectorDbFields.CATEGORY.value
Scope = VectorDbFields.SCOPE.value


def make_source_unit_cache(**kw):
    return SourceUnitDB(fname=kw.get('db_file', None))


def make_chunk_cache(**kw):
    return ChunkDB(model_name=kw['model_name'], fname=kw.get('db_file', None))


def get_connection(fname=None):
    global DbConnection
    if DbConnection is None:
        try:
            DbConnection = sqlite3.connect(':memory:' if fname is None
                                           else fname)
            DbConnection.row_factory = sqlite3.Row
        except sqlite3.Error as e:
            print(e)
    return DbConnection


def close_connection():
    global DbConnection
    if DbConnection is not None:
        DbConnection.close()
        DbConnection = None


def timestamp_str(ts, default=''):
    return T.timestamp_as_utc(ts).strftime('%Y-%m-%d %H:%M:%S.%f') if ts else default


def timestamps_to_datetimes(row: Optional[sqlite3.Row]) -> Optional[Dict[str, Any]]:
    def _utc_ts(ts):
        if not ts:
            return None
        return T.timestamp_as_utc(ts + ('+00:00' if '+' not in ts else ''))

    if row is not None:
        out = dict(row)
        out['last_edited_timestamp'] = _utc_ts(out['last_edited_timestamp'])
        out['added_timestamp'] = _utc_ts(out['added_timestamp'])
        out['embedded_timestamp'] = _utc_ts(out['embedded_timestamp'])
        out['metadata'] = json.loads(out['metadata']) or {}
        out['segments'] = pickle.loads(out['segments']) or []
        return out
    return None


class SourceUnitDB(Cache):
    def __init__(self, fname=None):
        self.conn = get_connection(fname)
        self.create_table()
        self.create_history_table()
        self.fname = fname

    def create_table(self):
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS source_unit (
                {Source} TEXT,
                {Source_unit_id} TEXT,
                uri TEXT,
                created_by TEXT,
                last_edited_by TEXT,
                last_edited_timestamp TIMESTAMP,
                added_timestamp TIMESTAMP,
                embedded_timestamp TIMESTAMP,
                {Category} TEXT,
                {Scope} TEXT,
                summary TEXT,
                segments BLOB,
                metadata TEXT,
                PRIMARY KEY({Source_unit_id}, {Source})
            )
        """)

    def reset_tables(self, only_in_memory=True):
        if not (self.fname is None and only_in_memory):
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
        self.conn.execute(f"""
            CREATE TABLE IF NOT EXISTS source_unit_history (
                {Source} TEXT,
                {Source_unit_id} TEXT,
                uri TEXT,
                created_by TEXT,
                last_edited_by TEXT,
                last_edited_timestamp TIMESTAMP,
                added_timestamp TIMESTAMP,
                embedded_timestamp TIMESTAMP,
                {Category} TEXT,
                {Scope} TEXT,
                summary TEXT,
                segments BLOB,
                metadata TEXT,
                deleted TIMESTAMP,
                PRIMARY KEY({Source_unit_id}, {Source}, deleted)
            )
        """)

    def delete(self, source: str, source_unit_id: str):
        existing_record = self.get(source, source_unit_id)
        if existing_record:
            query = """
                INSERT INTO source_unit_history
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(query, (
                existing_record[Source],
                existing_record[Source_unit_id],
                existing_record['uri'],
                existing_record['created_by'],
                existing_record['last_edited_by'],
                existing_record['last_edited_timestamp'],
                existing_record['added_timestamp'],
                existing_record['embedded_timestamp'],
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

    def add_or_update(self,
                      uri: str,
                      created_by: str,
                      last_edited_by: str,
                      last_edited_timestamp: datetime,
                      summary: str,
                      segments: List[Segment],
                      metadata: Dict = None,
                      embedded_timestamp: datetime = None,
                      **vector_db_fields):

        existing_record = self.get(vector_db_fields[Source],
                                   vector_db_fields[Source_unit_id])
        if existing_record:
            added_timestamp = existing_record['added_timestamp']
            self.delete(vector_db_fields[Source],
                        vector_db_fields[Source_unit_id])
        else:
            added_timestamp = datetime.now(utc)

        query = """
            INSERT OR REPLACE INTO source_unit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query, (vector_db_fields[Source],
                                  vector_db_fields[Source_unit_id],
                                  T.validate_uri(uri),
                                  created_by,
                                  last_edited_by,
                                  timestamp_str(last_edited_timestamp),
                                  timestamp_str(added_timestamp),
                                  timestamp_str(embedded_timestamp, default=None),
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
            WHERE embedded_timestamp IS NULL
            OR embedded_timestamp < last_edited_timestamp
        """)
        rows = cursor.fetchall()
        return [timestamps_to_datetimes(row) for row in rows]

    def flag_as_embedded(self, rows: List[Dict[str, Any]], now: datetime = None):
        query = f"""
            UPDATE source_unit
            SET embedded_timestamp = ?
            WHERE {Source} = ? AND {Source_unit_id} = ?
        """
        now = timestamp_str(now or datetime.now(utc))
        for row in rows:
            self.conn.execute(query, (now, row[Source], row[Source_unit_id]))
        self.conn.commit()

    def get_last_edited_timestamp(self,
                                  source: str,
                                  source_unit_id: str) -> Optional[datetime]:
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
    def __init__(self, model_name, fname=None):
        self.conn = get_connection(fname)
        self.table_name = 'chunk_' + model_name.replace('-', '_')

        self.create_table()
        self.fname = fname

    def create_table(self):
        self.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
          chunk_id TEXT,
          {Source} TEXT,
          {Source_unit_id} TEXT,
          text TEXT,
          vector BLOB,
          payload TEXT,
          vector_db_id TEXT,
          added_timestamp TIMESTAMP,
          PRIMARY KEY(chunk_id, {Source}, {Source_unit_id}),
          FOREIGN KEY({Source}, {Source_unit_id}) REFERENCES source_unit({Source}, {Source_unit_id})
        )
        """)
        self.conn.execute(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS idx_{self.table_name}_chunk_id
        ON {self.table_name} ({Source}, {Source_unit_id})
        """)

    def add_or_update(self,
                      source: str,
                      source_unit_id: str,
                      chunks: List[Chunk],
                      now=None):
        try:
            self.conn.executemany(f"""
                INSERT OR REPLACE INTO {self.table_name}
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, [(str(uuid.uuid5(uuid.NAMESPACE_URL, chunk.text)),
                   source,
                   source_unit_id,
                   chunk.text,
                   pickle.dumps(chunk.vector),
                   json.dumps(chunk.payload or {}),
                   chunk.vector_db_id,
                   timestamp_str(now or datetime.now(utc)))
                  for chunk in chunks])
            self.conn.commit()
        except sqlite3.Error as e:
            print(e)

    # FIXME It should get the source and source_unit_id as well, to enable the get_unembedded
    def get_most_recent_addition_datetime(self) -> Optional[Chunk]:
        """We can use this as the timestamp for
        SourceUnitDB.get_unembedded.  It enables quick selection of
        all the unembedded source units for any given model.
        """
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} ORDER BY added_timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if not row:
            return None

        return T.timestamp_as_utc(
            row['added_timestamp'] + ('+00:00' if '+' not in row['added_timestamp']
                                      else ''))

    def get(self, chunk_id: str) -> Optional[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE chunk_id=?", (chunk_id,))
        row = cursor.fetchone()
        return Chunk(row['text'],
                     vector=pickle.loads(row['vector']),
                     payload=json.dumps(row['payload']),
                     chunk_id=row['chunk_id'],
                     vector_db_id=row['vector_db_id']) if row else None

    def get_by_source_unit(self, source: str, source_unit_id: str) -> List[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE source=? AND source_unit_id=?",
                       (source, source_unit_id))
        rows = cursor.fetchall()
        return [Chunk(row['text'],
                      vector=pickle.loads(row['vector']),
                      payload=json.dumps(row['payload']),
                      chunk_id=row['chunk_id'],
                      vector_db_id=row['vector_db_id'])
                for row in rows]

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
