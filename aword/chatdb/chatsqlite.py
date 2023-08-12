# -*- coding: utf-8 -*-
"""Chat database. It stores conversations.
"""

from typing import Dict, List
import uuid
import logging
import sqlite3

import aword.errors as E

from aword.chatdb.chatdb import ChatDB


DbConnection = None


def make_chat_db(**kw):
    return ChatSQLite(db_file=kw.get('db_file', None))


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


class ChatSQLite(ChatDB):

    def __init__(self, db_file=None):
        self.db_file = db_file
        self.connection = get_connection(self.db_file)
        self._init_tables()

    def _init_tables(self):
        cursor = self.connection.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS chat (
            id TEXT PRIMARY KEY,
            tenant_id TEXT NOT NULL,
            user_id TEXT NOT NULL
        )
        ''')

        cursor.execute('''
        CREATE TABLE IF NOT EXISTS message (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            chat_id TEXT NOT NULL,
            role TEXT NOT NULL,
            content TEXT NOT NULL,
            FOREIGN KEY (chat_id) REFERENCES chat(id)
        )
        ''')
        self.connection.commit()

    def new_chat(self, tenant_id: str, user_id: str) -> str:
        chat_id = str(uuid.uuid4())
        cursor = self.connection.cursor()
        cursor.execute('''
        INSERT INTO chat (id, tenant_id, user_id) VALUES (?, ?, ?)
        ''', (chat_id, tenant_id, user_id))
        self.connection.commit()
        return chat_id

    def append_messages(self, chat_id: str, messages: List[Dict]):
        cursor = self.connection.cursor()
        for message in messages:
            cursor.execute('''
            INSERT INTO message (chat_id, role, content) VALUES (?, ?, ?)
            ''', (chat_id, message['role'], message['content']))
        self.connection.commit()

    def get_messages(self, chat_id: str) -> List[Dict]:
        cursor = self.connection.cursor()
        cursor.execute('''
        SELECT role, content FROM message WHERE chat_id = ?
        ''', (chat_id,))
        rows = cursor.fetchall()
        return [{'role': row['role'], 'content': row['content']} for row in rows]
