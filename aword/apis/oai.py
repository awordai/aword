# -*- coding: utf-8 -*-

import os
from typing import List

import openai

from tenacity import retry, wait_random_exponential
from tenacity import stop_after_attempt, retry_if_not_exception_type

import aword.tools as T

C = None


def ensure_api():
    global C
    if C is None:
        T.load_environment()
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            raise RuntimeError('Missing openai api key in OPENAI_API_KEY')
        openai.api_key = api_key
        C = T.get_config('openai')


@retry(wait=wait_random_exponential(min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_not_exception_type(openai.InvalidRequestError))
def get_embeddings(text_or_tokens_array, model=None) -> List[List[float]]:
    ensure_api()
    return [r['embedding'] for r in
            openai.Embedding.create(input=text_or_tokens_array,
                                    model=model or C['oai_embedding_model'])["data"]]


@retry(wait=wait_random_exponential(min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_not_exception_type(openai.InvalidRequestError))
def ask_question(sytem_prompt, user_prompt):
    ensure_api()
    return openai.ChatCompletion.create(
        model=C['oai_model'],
        messages=[
            {"role": "system", "content": sytem_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]
