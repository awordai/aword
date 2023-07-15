# -*- coding: utf-8 -*-

from typing import Callable, Any, List, Dict

from aword.apis import oai


def make_summarizer(config: Dict):
    provider = config.get('provider')

    if provider == 'openai':
        return OAISummarizer(**config)

    raise ValueError(f"Unknown model provider '{provider}'")


class OAISummarizer:

    def __init__(self,
                 model_name: str,
                 summary_words: int,
                 system_prompt_file: str,
                 **_):

        self.model_name = model_name
        self.summary_words = summary_words

        with open(system_prompt_file, encoding='utf-8') as fin:
            self.system_prompt = fin.read() + (
                f'  The summary should not contain more than {summary_words} words.'
            )

    def summarize(self, text):
        if len(text.split()) <= self.summary_words:
            return text

        return oai.chat(model_name=self.model_name,
                        system_prompt=self.system_prompt,
                        user_prompt=f'Summarize the following text:\n\n```\n{text}\n```')
