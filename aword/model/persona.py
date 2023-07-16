# -*- coding: utf-8 -*-

import string
from typing import Dict

from aword.apis import oai


def make_persona(config: Dict):
    provider = config.get('provider', 'openai')

    if provider == 'openai':
        return OAIPersona(**config)

    raise ValueError(f"Unknown model provider '{provider}'")


class OAIPersona:

    def __init__(self,
                 model_name: str,
                 system_prompt: str,
                 user_prompt_preface: str = '',
                 **params):
        self.model_name = model_name
        self.params = params
        self.system_prompt = string.Template(system_prompt).substitute(params)
        self.user_prompt_preface = string.Template(user_prompt_preface).substitute(params)

    def get_param(self, parname: str):
        return self.params[parname]

    def ask(self, text: str):
        return oai.chat(model_name=self.model_name,
                        system_prompt=self.system_prompt,
                        user_prompt=self.user_prompt_preface + text)
