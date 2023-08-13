# -*- coding: utf-8 -*-
"""Ask questions to a respondent.
"""
import string
import time
from typing import Dict

from aword.apis import oai
import aword.errors as E

def make_respondent(awd,
                    respondent_name: str,
                    config: Dict):
    provider = config.get('provider', 'openai')

    if provider == 'openai':
        return OAIRespondent(awd, respondent_name, **config)

    raise ValueError(f"Unknown model provider '{provider}'")


class OAIRespondent:

    def __init__(self,
                 awd,
                 respondent_name: str,
                 model_name: str,
                 system_prompt: str,
                 call_function: Dict,
                 user_prompt_preface: str = '',
                 **params):
        oai.ensure_api(awd.getenv('OPENAI_API_KEY'))

        self.logger = awd.logger
        self.respondent_name = respondent_name
        self.model_name = model_name
        self.params = params

        self.system_prompt = string.Template(system_prompt).substitute(params)
        self.user_prompt_preface = string.Template(user_prompt_preface).substitute(params)

        for argument in call_function['parameters']['properties'].values():
            argument['description'] = string.Template(argument['description']).substitute(params)
        self.call_function = call_function

    def get_param(self, parname: str):
        return self.params[parname]

    def ask(self,
            text: str,
            temperature: float = 1,
            attempts: int = 2):
        self.logger.info('Asked @%s: %s',
                         self.respondent_name,
                         text[:80].replace('\n', ' '))
        try:
            out = oai.chat_completion_request(
                messages=[{'role': 'system',
                           'content': self.system_prompt},
                          {'role': 'user',
                           'content': self.user_prompt_preface + text}],
                functions=[self.call_function],
                call_function=self.call_function['name'],
                model_name=self.model_name,
                temperature=temperature)
        except Exception as e:
            # This means that the request was not correctly formed, do not try again
            if isinstance(e, E.AwordModelRequestError):
                raise
            if attempts:
                time.sleep(0.5)
                return self.ask(text, temperature, attempts-1)

            out = {'success': False,
                   'reply': 'Failed at generating reply'}

        self.logger.info('Replied @%s: %s, %s',
                         self.respondent_name,
                         'success' if out['success'] else 'failure',
                         out['reply'][:80])
        return out


def add_args(parser):
    import argparse
    parser.add_argument('respondent', help='Respondent')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    respondent_name = args['respondent'].replace('@', '')
    question = ' '.join(args['question'])
    respondent = awd.get_respondent(respondent_name)
    print(respondent.ask(question))
