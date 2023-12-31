# -*- coding: utf-8 -*-
"""Chat with a persona.
"""

import os
from abc import ABC, abstractmethod
from typing import Dict, List
import threading
import time
import gnureadline as readline


class Chat(ABC):
    def __init__(self, awd):
        self.awd = awd

    @abstractmethod
    def new_chat(self, user_id: str) -> str:
        """Create a new chat for a vector_namespace and a user_id. It should
        generate a unique id for the chat and return it.
        """

    @abstractmethod
    def append_messages(self, chat_id: str, messages: List[Dict]):
        """Append a list of messages to a chat. It should create a row
        for each message. Messages are dictionaries with a 'role'
        ('system', 'user', 'assistant', 'function') and 'content'.
        """

    @abstractmethod
    def get_messages(self, chat_id: str) -> List[Dict]:
        """Return the messages belonging to a chat. Most recent last."""

    def user_says(
        self,
        persona_name: str,
        user_id: str,
        user_query: str,
        chat_id: str = None,
        attempts: int = 2,
    ) -> Dict:
        """Receives input from the user, and replies. The reply is a
        dictionary with an entry 'chat_id' that will be used to refer
        to the chat later, a boolean entry 'success', an an entry
        'reply' with the text of the reply.
        """
        if chat_id is None:
            message_history = []
        else:
            message_history = self.get_messages(chat_id)

        persona = self.awd.get_persona(persona_name)
        try:
            reply = persona.tell(user_query, message_history)
        except Exception as e:
            self.awd.logger.error(
                'Failed getting reply from %s, chat_id %s, user_query %s: \n%s',
                persona_name,
                chat_id,
                user_query,
                str(e),
            )
            return {
                'reply': f'Failed at getting reply from {persona_name} for {user_query}',
                'success': False,
                'chat_id': chat_id,
            }

        if reply['success']:
            self.awd.logger.info('Storing messages')
            self.append_messages(
                chat_id=chat_id,
                messages=[
                    {
                        'role': 'user',
                        'said': reply['user_says'],
                        'background': reply['background'],
                    },
                    {'role': 'assistant', 'name': persona_name, 'said': reply['reply']},
                ],
            )
            reply['chat_id'] = chat_id
            return reply

        if attempts:
            return self.user_says(
                persona_name=persona_name,
                user_id=user_id,
                user_query=user_query,
                chat_id=chat_id,
                attempts=attempts - 1,
            )

        return reply

    def cli_chat(self, persona_name: str, user_id: str, chat_id: str = None):
        """
        A command line chat interface using readline.

        Parameters:
        - persona_name: The name of the persona.
        - user_id: The ID of the user.
        """

        if os.path.exists(os.path.expanduser('~/.inputrc')):
            # pylint: disable=c-extension-no-member
            readline.read_init_file(os.path.expanduser('~/.inputrc'))

        history_dir = os.path.expanduser('~/.aword')
        if not os.path.exists(history_dir):
            os.makedirs(history_dir)
        history_file = os.path.join(history_dir, 'chat-history')

        if os.path.exists(history_file):
            readline.read_history_file(history_file)

        chat_id = chat_id or self.new_chat(user_id)

        print(f"Hi, this is {persona_name}. Type 'exit' or 'x' to quit.")

        response = None  # Place to store the eventual response
        error = None  # Place to store any error that occurs

        def thinking_animation():
            """Display a simple animation indicating the system is 'thinking'."""
            # Hide cursor
            print('\033[?25l', end='', flush=True)

            i = 0
            animation_length = 20
            while not response and not error:
                print((i % animation_length) * '·', end='\r', flush=True)
                time.sleep(1)
                i += 1
                if i and not i % animation_length:
                    print('\r' + ' ' * animation_length + '\r', end='', flush=True)

            # Show cursor
            print('\033[?25h', end='', flush=True)
            # Clear the 'thinking' line
            print('\r' + ' ' * animation_length + '\r', end='', flush=True)

        while True:
            user_query = input('> ')

            # If user types 'exit', end the chat
            if user_query.lower() in ('exit', 'x'):
                break

            if not user_query.strip():
                continue

            readline.write_history_file(history_file)

            # Start the thinking animation on a separate thread
            animation_thread = threading.Thread(target=thinking_animation)
            animation_thread.start()

            # Get the response on the main thread
            try:
                response = self.user_says(
                    persona_name=persona_name,
                    user_id=user_id,
                    user_query=user_query,
                    chat_id=chat_id,
                )
                if response['success']:
                    print(response['reply'])
                else:
                    print("Sorry, couldn't process the message. Try again.")
            except Exception as e:
                error = str(e)
                print(f"Error: {error}")

            # Ensure thinking animation stops
            animation_thread.join()

            # Reset response and error for the next iteration
            response = None
            error = None

        print("Goodbye!")

    def tell(self, persona_name: str, user_query: str):
        persona = self.awd.get_persona(persona_name)
        try:
            reply = persona.tell(user_query, message_history=[])
        except Exception as e:
            self.awd.logger.error(
                'Failed getting reply from %s, user_query %s: \n%s',
                persona_name,
                user_query,
                str(e),
            )
            return {
                'reply': f'Failed at getting reply from {persona_name} for {user_query}',
                'success': False,
            }
        return reply


def add_args(parser):
    import getpass
    import argparse

    parser.add_argument('--user-id', help=('User id'), type=str, default=getpass.getuser())
    parser.add_argument('--single-question', help='Ask a single question.', action='store_true')
    parser.add_argument('persona', help='Persona')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    from pprint import pprint

    chat = awd.get_chat()
    persona_name = args['persona'].replace('@', '')
    if args['single_question']:
        question = ' '.join(args['question'])
        if len(question) < 5:
            awd.logger.error('The question is too short')
        else:
            pprint(chat.tell(persona_name, question))
    else:
        chat.cli_chat(persona_name=persona_name, user_id=args['user_id'])
