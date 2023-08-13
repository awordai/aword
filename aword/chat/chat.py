# -*- coding: utf-8 -*-
"""Chat database. It stores conversations.
"""

from abc import ABC, abstractmethod
from typing import Dict, List


class Chat(ABC):

    def __init__(self, awd):
        self.awd = awd

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
        ('system', 'user', 'assistant', 'function') and 'content'.
        """

    @abstractmethod
    def get_messages(self, chat_id: str) -> List[Dict]:
        """Return the messages belonging to a chat. Most recent last.
        """

    def user_says(self,
                  persona_name: str,
                  tenant_id: str,
                  user_id: str,
                  user_query: str,
                  chat_id: str = None) -> Dict:
        if chat_id is None:
            chat_id = self.new_chat(tenant_id, user_id)
            message_history = []
        else:
            message_history = self.get_messages(chat_id)

        persona = self.awd.get_persona(persona_name)
        try:
            reply = persona.tell(user_query, message_history)
            self.append_messages(chat_id=chat_id,
                                 messages=[{'role': 'user',
                                            'content': user_query},
                                           {'role': 'assistant',
                                            'name': persona_name,
                                            'content': reply}])
        except Exception as e:
            self.awd.logger.error('Failed getting reply from %s, chat_id %s, user_query %s: \n%s',
                                  persona_name, chat_id, user_query, str(e))
            return {'reply': f'Failed at getting reply from {persona_name}',
                    'success': False,
                    'chat_id': chat_id}

        return {'reply': reply,
                'success': True,
                'chat_id': chat_id}


def add_args(parser):
    import argparse
    parser.add_argument('persona', help='Persona')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    persona_name = args['persona'].replace('@', '')
    question = ' '.join(args['question'])
    persona = awd.get_persona(persona_name)
    print(persona.chat(question))
