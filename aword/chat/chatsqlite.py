# -*- coding: utf-8 -*-
"""Chat database. It stores conversations.
"""

from typing import Dict, List
import uuid
import logging
import sqlite3
from datetime import datetime
from pytz import utc

import aword.errors as E

from aword.chat.chat import Chat


DbConnection = None


def make_chat(awd, **kw):
    return ChatSQLite(awd, db_file=kw.get('db_file', None))


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


class ChatSQLite(Chat):

    def __init__(self, awd, db_file=None):
        super().__init__(awd)
        self.vector_namespace = awd.get_vector_namespace()
        awd.logger.info('Initializing sqlite chat for vector namespace %s', self.vector_namespace)
        self.db_file = db_file
        self.connection = get_connection(self.db_file)
        self._init_tables()

    def _init_tables(self):
        cursor = self.connection.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat (
            id TEXT PRIMARY KEY,
            vector_namespace TEXT NOT NULL,
            user_id TEXT NOT NULL,
            created_timestamp TIMESTAMP
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS message (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            role TEXT NOT NULL,
            said TEXT NOT NULL,
            background TEXT,
            model TEXT,
            prompt_tokens INTEGER,
            completion_tokens INTEGER,
            total_tokens INTEGER,
            created_timestamp TIMESTAMP,
            FOREIGN KEY (chat_id) REFERENCES chat(id)
        )
        ''')
        self.connection.commit()

    def new_chat(self, user_id: str) -> str:
        chat_id = str(uuid.uuid4())
        cursor = self.connection.cursor()
        cursor.execute('''
        INSERT INTO chat (id, vector_namespace, user_id, created_timestamp) VALUES (?, ?, ?, ?)
        ''', (chat_id, self.vector_namespace, user_id, datetime.now(utc).isoformat()))
        self.connection.commit()
        return chat_id

    def append_messages(self, chat_id: str, messages: List[Dict]):
        cursor = self.connection.cursor()
        for message in messages:
            cursor.execute('''
            INSERT INTO message (chat_id,
                                 role,
                                 said,
                                 background,
                                 model,
                                 prompt_tokens,
                                 completion_tokens,
                                 total_tokens,
                                 created_timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            ''', (chat_id,
                  message['role'],
                  message['said'],
                  message.get('background', ''),
                  message.get('model', ''),
                  message.get('prompt_tokens', None),
                  message.get('completion_tokens', None),
                  message.get('total_tokens', None),
                  datetime.now(utc).isoformat()))
        self.connection.commit()

    def get_messages(self, chat_id: str) -> List[Dict]:
        """Returns the messages belonging to a chat, most recent last.

        TODO: querying by chat_id is a potential vulnerability. If a
        user's permisions to access a vector_namespace are revoked
        they could still access it if they knew a chat_id.
        """
        cursor = self.connection.cursor()
        cursor.execute('''
        SELECT role, said, background,total_tokens FROM message WHERE chat_id = ?
        ORDER BY created_timestamp ASC
        ''', (chat_id,))
        rows = cursor.fetchall()
        return [{'role': row['role'],
                 'said': row['said'],
                 'background': row['background'],
                 'total_tokens': row['total_tokens']} for row in rows]
