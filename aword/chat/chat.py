# -*- coding: utf-8 -*-
"""Chat database. It stores conversations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List


class Chat(ABC):

    @abstractmethod
    def new_chat(self,
                 tenant_id: str,
                 user_id: str) -> str:
        """Create a new chat for a tenant_id and a user_id. It should
        generate a unique id for the chat and return it.
        """

    @abstractmethod
    def append_messages(self,
                        chat_id: str,
                        messages: List[Dict]):
        """Append a list of messages to a chat. It should create a row
        for each message. Messages are dictionaries with a 'role'
        ('entry', 'user', 'assistant') and 'content'.
        """

    @abstractmethod
    def get_messages(self, chat_id: str) -> List[Dict]:
        """Return the messages belonging to a chat.
        """


def add_args(parser):
    import argparse
    parser.add_argument('persona', help='Persona')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    persona_name = args['persona'].replace('@', '')
    question = ' '.join(args['question'])
    persona = awd.get_persona(persona_name)
    print(persona.chat(question))
