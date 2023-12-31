# -*- coding: utf-8 -*-

import pickle
import json
from itertools import groupby
import uuid
import logging

import sqlite3
from sqlite3 import Error
from datetime import datetime
from typing import Optional, Dict, Any, List, Union

from pytz import utc

import aword.errors as E
import aword.tools as T
from aword.segment import Segment
from aword.chunk import Payload, Chunk
from aword.cache.cache import Cache, combine_segments, guess_language


DbConnection = None


def make_source_unit_cache(summarizer=None, **kw):
    return SourceUnitDB(summarizer, db_file=kw.get('db_file', None))


def make_chunk_cache(**kw):
    return ChunkDB(db_file=kw.get('db_file', None))


def get_connection(fname=None):
    global DbConnection
    if DbConnection is None:
        logger = logging.getLogger(__name__)
        try:
            connect_to = ':memory:' if fname is None else fname
            DbConnection = sqlite3.connect(connect_to)
            DbConnection.row_factory = sqlite3.Row
            logger.info('Created sqlite connection to %s', connect_to)
        except sqlite3.Error as exc:
            logger.error('Failed trying to connect to sqlite database %s', fname)
            raise E.AwordError(f'Cannot connect to sqlite database {fname}') from exc
    return DbConnection


def close_connection():
    global DbConnection
    if DbConnection is not None:
        DbConnection.close()
        DbConnection = None


def timestamp_str(ts, default=''):
    return T.timestamp_as_utc(ts).isoformat() if ts else default
    # return T.timestamp_as_utc(ts).strftime('%Y-%m-%d %H:%M:%S.%f') if ts else default


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
        out['categories'] = json.loads(out['categories']) or []
        out['segments'] = pickle.loads(out['segments']) or []
        return out
    return None


def limit_query(query: str,
                source: str = None,
                source_unit_id: str = None,
                args: List[str] = None):
    args = args or []
    log_str = ''
    if source is not None:
        if 'WHERE' in query:
            query += ' AND (source = ?'
        else:
            query += ' WHERE (source = ?'
        args.append(source)
        log_str = f' (source: {source}'
        if source_unit_id is None:
            query += ')'
        else:
            query += ' AND source_unit_id = ?)'
            args.append(source_unit_id)
            log_str += f', source_unit_id: {source_unit_id}'
        log_str += ')'
    return query, args, log_str


class SourceUnitDB(Cache):
    def __init__(self, summarizer=None, db_file=None):
        super().__init__(summarizer)

        self.conn = get_connection(db_file)
        self.logger = logging.getLogger(__name__)
        self.create_table()
        self.create_history_table()
        self.db_file = db_file

    def create_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source_unit (
                source TEXT,
                source_unit_id TEXT,
                uri TEXT,
                created_by TEXT,
                last_edited_by TEXT,
                last_edited_timestamp TIMESTAMP,
                added_timestamp TIMESTAMP,
                embedded_timestamp TIMESTAMP,
                categories TEXT,
                scope TEXT,
                context TEXT,
                language TEXT,
                summary TEXT,
                segments BLOB,
                metadata TEXT,
                PRIMARY KEY(source_unit_id, source)
            )
        """)
        self.logger.info('Attempted source_unit table creation')

    def reset_tables(self, only_in_memory=True):
        logger = logging.getLogger(__name__)

        if not (self.db_file is None and only_in_memory):
            logger.warning('Refusing to drop persistent tables without `only_in_memory` argument')
            return
        try:
            self.conn.execute("DROP TABLE IF EXISTS source_unit")
            self.conn.execute("DROP TABLE IF EXISTS source_unit_history")
            self.logger.info('Dropped tables source_unit and source_unit_history')
            self.create_table()
            self.create_history_table()
        except Error as e:
            logger.error('Failed trying to create source_unit and source_unit_history tables')
            raise E.AwordError('Failed trying to create source_unit and '
                               'source_unit_history tables') from e

    def create_history_table(self):
        self.conn.execute("""
            CREATE TABLE IF NOT EXISTS source_unit_history (
                source TEXT,
                source_unit_id TEXT,
                uri TEXT,
                created_by TEXT,
                last_edited_by TEXT,
                last_edited_timestamp TIMESTAMP,
                added_timestamp TIMESTAMP,
                embedded_timestamp TIMESTAMP,
                categories TEXT,
                scope TEXT,
                context TEXT,
                language TEXT,
                summary TEXT,
                segments BLOB,
                metadata TEXT,
                deleted TIMESTAMP,
                PRIMARY KEY(source_unit_id, source, deleted)
            )
        """)
        self.logger.info('Attempted source_unit_history table creation')

    def delete(self, source: str, source_unit_id: str):
        existing_record = self.get(source, source_unit_id)
        if existing_record:
            query = """
                INSERT OR REPLACE INTO source_unit_history
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """
            self.conn.execute(query, (
                existing_record['source'],
                existing_record['source_unit_id'],
                existing_record['uri'],
                existing_record['created_by'],
                existing_record['last_edited_by'],
                existing_record['last_edited_timestamp'],
                existing_record['added_timestamp'],
                existing_record['embedded_timestamp'],
                json.dumps(existing_record['categories']),
                existing_record['scope'],
                existing_record['context'],
                existing_record['language'],
                existing_record['summary'],
                pickle.dumps(existing_record['segments']),
                json.dumps(existing_record['metadata'], sort_keys=True),
                timestamp_str(datetime.now(utc)),
            ))
            self.conn.execute("DELETE FROM source_unit WHERE "
                              "source = ? AND "
                              "source_unit_id = ?",
                              (source, source_unit_id))
            self.conn.commit()
            self.logger.info('Deleted record (%s, %s)', source, source_unit_id)
        else:
            self.logger.info('Attempting to delete non-existing record (%s, %s)',
                             source, source_unit_id)

    def add_or_update(self,
                      source: str,
                      source_unit_id: str,
                      uri: str,
                      created_by: str,
                      last_edited_by: str,
                      last_edited_timestamp: datetime,
                      segments: List[Segment],
                      categories: Union[List[str], str] = '[]',
                      scope: str = '',
                      context: str = '',
                      language: str = '',
                      summary: str = '',
                      metadata: Dict = None):

        source_unit_text = combine_segments(segments)

        existing_record = self.get(source, source_unit_id)
        if existing_record:
            added_timestamp = existing_record['added_timestamp']
            self.delete(source, source_unit_id)
        else:
            added_timestamp = datetime.now(utc)

        # Make sure that a new source unit will be unembedded
        embedded_timestamp = None

        query = """
            INSERT OR REPLACE INTO source_unit
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """
        self.conn.execute(query,
                          (source,
                           source_unit_id,
                           T.validate_uri(uri),
                           created_by,
                           last_edited_by,
                           timestamp_str(last_edited_timestamp),
                           timestamp_str(added_timestamp),
                           timestamp_str(embedded_timestamp, default=None),
                           categories if isinstance(categories, str) else json.dumps(categories),
                           scope,
                           context,
                           language or guess_language(source_unit_text),
                           summary or self.summarize(source_unit_text),
                           pickle.dumps(segments),
                           json.dumps(metadata, sort_keys=True)))
        self.conn.commit()
        self.logger.info('Inserted or replaced in source_unit (%s, %s), last_edited_timestamp %s',
                         source, source_unit_id, timestamp_str(last_edited_timestamp))
        return source_unit_id

    def count_rows(self,
                   source: str = None,
                   source_unit_id: str = None) -> int:
        cursor = self.conn.cursor()
        query, args, _ = limit_query('SELECT COUNT(*) FROM source_unit', source, source_unit_id)
        cursor.execute(query, args)
        # fetchone will return a tuple with one element
        return cursor.fetchone()[0]

    def get_by_uri(self, source: str, uri: str) -> Optional[Dict[str, Any]]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit WHERE uri=? AND source=?", (uri, source))
        return timestamps_to_datetimes(cursor.fetchone())

    def list_rows(self,
                  source: str = None,
                  source_unit_id: str = None) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        query, args, _ = limit_query('SELECT * FROM source_unit', source, source_unit_id)
        cursor.execute(query, args)
        return [timestamps_to_datetimes(dict(row)) for row in cursor.fetchall()]

    def get(self, source: str, source_unit_id: str) -> Optional[Dict[str, Any]]:
        rows = self.list_rows(source, source_unit_id)
        return rows[0] if rows else None

    def list_unembedded_rows(self,
                             source: str = None) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        query, args, _ = limit_query(('SELECT * FROM source_unit '
                                      'WHERE embedded_timestamp IS NULL '
                                      'OR embedded_timestamp < last_edited_timestamp'),
                                     source)
        cursor.execute(query, args)
        return [timestamps_to_datetimes(row) for row in cursor.fetchall()]

    def flag_as_embedded(self, rows: List[Dict[str, Any]], now: datetime = None):
        query = """
            UPDATE source_unit
            SET embedded_timestamp = ?
            WHERE source = ? AND source_unit_id = ?
        """
        now = timestamp_str(now or datetime.now(utc))
        for row in rows:
            self.conn.execute(query, (now, row['source'], row['source_unit_id']))
        self.conn.commit()

    def reset_embedded(self,
                       source: str = None,
                       source_unit_id: str = None):
        query, args, logstr = limit_query('UPDATE source_unit SET embedded_timestamp = null',
                                          source,
                                          source_unit_id)
        self.conn.execute(query, args)
        logger = logging.getLogger(__name__)
        logger.info('Resetted last embedded datetime%s', logstr)
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
        if row:
            self.logger.debug('Found row for (%s, %s)', source, source_unit_id)
            return T.timestamp_as_utc(row['last_edited_timestamp'])
        self.logger.debug('Could not find row for (%s, %s)', source, source_unit_id)
        return None

    def get_most_recent_last_edited_timestamp(self) -> Optional[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute("SELECT * FROM source_unit ORDER BY last_edited_timestamp DESC LIMIT 1")
        row = cursor.fetchone()
        if not row:
            return None

        return T.timestamp_as_utc(
            row['last_edited_timestamp'] + ('+00:00' if '+' not in row['last_edited_timestamp']
                                            else ''))

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
                                key=lambda row: (row['source_unit_id'],
                                                 row['source'])):
            grouped_records.append(next(group))

        # Step 3
        all_records = {(row['source_unit_id'],
                        row['source']): row for row in current_records}
        all_records.update({(row['source_unit_id'],
                             row['source']): row
                            for row in grouped_records})

        return list(all_records.values())


class ChunkDB:

    def __init__(self, db_file=None):
        self.conn = get_connection(db_file)
        self.table_name = 'chunk'

        self.logger = logging.getLogger(__name__)

        self.create_table()
        self.db_file = db_file

    def create_table(self):
        self.conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {self.table_name} (
          chunk_id TEXT,
          source TEXT,
          source_unit_id TEXT,
          vector BLOB,
          payload TEXT,
          vector_db_id TEXT,
          added_timestamp TIMESTAMP,
          PRIMARY KEY(source, source_unit_id, chunk_id),
          FOREIGN KEY(source, source_unit_id) REFERENCES source_unit(source, source_unit_id)
        )
        """)
        self.logger.info('Attempted %s table creation', self.table_name)

    def reset_table(self, only_in_memory=True):
        logger = logging.getLogger(__name__)

        if self.db_file is not None and only_in_memory:
            logger.warning('Refusing to drop persistent chunk table '
                           'without `only_in_memory` argument')
            return
        try:
            self.conn.execute(f"DROP TABLE IF EXISTS {self.table_name}")
            self.logger.info('Dropped table %s', self.table_name)
            self.create_table()
        except Error as e:
            logger.error('Failed trying to recreate %s table', self.table_name)
            raise E.AwordError(f'Failed trying to create {self.table_name} table') from e

    def add(self,
            source: str,
            source_unit_id: str,
            chunks: List[Chunk],
            now=None):
        self.conn.executemany(f"""
        INSERT OR REPLACE INTO {self.table_name}
        VALUES (?, ?, ?, ?, ?, ?, ?)
        """, [(chunk.chunk_id or str(uuid.uuid5(uuid.NAMESPACE_URL, chunk.text)),
               source,
               source_unit_id,
               pickle.dumps(chunk.vector),
               json.dumps(chunk.payload),
               chunk.vector_db_id,
               timestamp_str(now or datetime.now(utc)))
              for chunk in chunks])
        self.conn.commit()
        self.logger.info('Inserted %d chunks in %s (%s, %s)',
                         len(chunks),
                         self.table_name,
                         source,
                         source_unit_id)

    def delete_source_unit(self, source: str, source_unit_id: str):
        self.conn.execute(f"DELETE FROM {self.table_name} WHERE "
                          "source = ? AND "
                          "source_unit_id = ?",
                          (source, source_unit_id))
        self.conn.commit()
        self.logger.info('Deleted source unit chunks (%s, %s)', source, source_unit_id)

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

    def count_rows(self,
                   source: str = None,
                   source_unit_id: str = None) -> int:
        cursor = self.conn.cursor()
        query, args, _ = limit_query(f'SELECT COUNT(*) FROM {self.table_name}',
                                     source,
                                     source_unit_id)
        cursor.execute(query, args)
        # fetchone will return a tuple with one element
        return cursor.fetchone()[0]

    def get(self, chunk_id: str) -> Optional[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE chunk_id=?", (chunk_id,))
        row = cursor.fetchone()
        return Chunk(vector=pickle.loads(row['vector']),
                     payload=Payload(**(json.loads(row['payload']))),
                     chunk_id=row['chunk_id'],
                     vector_db_id=row['vector_db_id']) if row else None

    def list_rows(self,
                  source: str = None,
                  source_unit_id: str = None) -> List[Dict[str, Any]]:
        cursor = self.conn.cursor()
        query, args, _ = limit_query(f'SELECT * FROM {self.table_name}',
                                     source,
                                     source_unit_id)
        cursor.execute(query, args)
        return [Chunk(vector=pickle.loads(row['vector']),
                      payload=Payload(**(json.loads(row['payload']))),
                      chunk_id=row['chunk_id'],
                      vector_db_id=row['vector_db_id'])
                for row in cursor.fetchall()]

    def get_by_source_unit(self, source: str, source_unit_id: str) -> List[Chunk]:
        cursor = self.conn.cursor()
        cursor.execute(f"SELECT * FROM {self.table_name} WHERE source=? AND source_unit_id=?",
                       (source, source_unit_id))
        rows = cursor.fetchall()
        return [Chunk(vector=pickle.loads(row['vector']),
                      payload=Payload(**(json.loads(row['payload']))),
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
            raise E.AwordError('Failed trying to resent vector_db_id by source unit '
                               f'({source}, {source_unit_id})') from e
