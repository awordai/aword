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
                  chat_id: str = None,
                  attempts: int = 2) -> Dict:
        if chat_id is None:
            chat_id = self.new_chat(tenant_id, user_id)
            message_history = []
        else:
            message_history = self.get_messages(chat_id)

        persona = self.awd.get_persona(persona_name)
        try:
            reply = persona.tell(user_query, message_history, collection_name=tenant_id)
        except Exception as e:
            self.awd.logger.error('Failed getting reply from %s, chat_id %s, user_query %s: \n%s',
                                  persona_name, chat_id, user_query, str(e))
            return {'reply': f'Failed at getting reply from {persona_name} for {user_query}',
                    'success': False,
                    'chat_id': chat_id}

        if reply['success']:
            self.awd.logger.info('Storing messages')
            self.append_messages(chat_id=chat_id,
                                 messages=[{'role': 'user',
                                            'said': reply['user_says'],
                                            'background': reply['background']},
                                           {'role': 'assistant',
                                            'name': persona_name,
                                            'said': reply['reply']}])
            reply['chat_id'] = chat_id
            return reply

        if attempts:
            return self.user_says(persona_name=persona_name,
                                  tenant_id=tenant_id,
                                  user_id=user_id,
                                  user_query=user_query,
                                  chat_id=chat_id,
                                  attempts=attempts - 1)

        return reply




def add_args(parser):
    import getpass
    import argparse
    parser.add_argument('--tenant-id',
                        help=('Tenant id'),
                        type=str,
                        default='local')
    parser.add_argument('--user-id',
                        help=('User id'),
                        type=str,
                        default=getpass.getuser())
    parser.add_argument('persona', help='Persona')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    chat = awd.get_chat()
    persona_name = args['persona'].replace('@', '')
    question = ' '.join(args['question'])
    from pprint import pprint
    pprint(chat.user_says(persona_name=persona_name,
                          tenant_id=args['tenant_id'],
                          user_id=args['user_id'],
                          user_query=question))
