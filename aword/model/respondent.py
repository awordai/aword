# -*- coding: utf-8 -*-

import string
from typing import Dict

from aword.apis import oai


def make_respondent(awd, config: Dict):
    provider = config.get('provider', 'openai')

    if provider == 'openai':
        return OAIRespondent(awd, **config)

    raise ValueError(f"Unknown model provider '{provider}'")


class OAIRespondent:

    def __init__(self,
                 awd,
                 model_name: str,
                 system_prompt: str,
                 call_function: Dict,
                 user_prompt_preface: str = '',
                 **params):
        oai.ensure_api(awd.getenv('OPENAI_API_KEY'))

        self.model_name = model_name
        self.params = params

        self.system_prompt = string.Template(system_prompt).substitute(params)
        self.user_prompt_preface = string.Template(user_prompt_preface).substitute(params)

        for argument in call_function['parameters']['properties'].values():
            argument['description'] = string.Template(argument['description']).substitute(params)
        self.call_function = call_function

    def get_param(self, parname: str):
        return self.params[parname]

    def ask(self, text: str,
            temperature: float = 1):
        return oai.chat_completion_request(
            messages=[{'role': 'system',
                       'content': self.system_prompt},
                      {'role': 'user',
                       'content': self.user_prompt_preface + text}],
            functions=[self.call_function],
            call_function=self.call_function['name'],
            model_name=self.model_name,
            temperature=temperature)


def add_args(parser):
    import argparse
    parser.add_argument('respondent', help='Respondent')
    parser.add_argument('question', nargs=argparse.REMAINDER, help='Question')


def main(awd, args):
    respondent_name = args['respondent'].replace('@', '')
    question = ' '.join(args['question'])
    respondent = awd.get_respondent(respondent_name)
    print(respondent.ask(question))
