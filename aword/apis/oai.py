# -*- coding: utf-8 -*-

import os
from typing import List, Any

import openai
import tiktoken

from tenacity import retry, wait_random_exponential
from tenacity import stop_after_attempt, retry_if_not_exception_type

import aword.tools as T


Api_loaded = False


def ensure_api():
    global Api_loaded
    if not Api_loaded:
        T.load_environment()
        api_key = os.environ.get('OPENAI_API_KEY')
        if not api_key:
            raise RuntimeError('Missing openai api key in OPENAI_API_KEY')
        openai.api_key = api_key
        Api_loaded = True


@retry(wait=wait_random_exponential(min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_not_exception_type(openai.InvalidRequestError))
def fetch_embeddings(text_or_tokens_array: List[str],
                     model_name: str) -> List[List[float]]:
    ensure_api()
    return [r['embedding'] for r in
            openai.Embedding.create(input=text_or_tokens_array,
                                    model=model_name)["data"]]


def get_embeddings(chunked_texts: List[str],
                   model_name: str) -> List[float]:
    # Split text_chunks into shorter arrays of max length 100
    max_batch_size = 100
    text_chunks_arrays = [chunked_texts[i:i+max_batch_size]
                          for i in range(0, len(chunked_texts), max_batch_size)]

    embeddings = []
    for text_chunks_array in text_chunks_arrays:
        embeddings += fetch_embeddings(text_chunks_array,
                                       model_name)

    return embeddings


@retry(wait=wait_random_exponential(min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_not_exception_type(openai.InvalidRequestError))
def ask_question(model_name: str,
                 sytem_prompt: str,
                 user_prompt: str):
    ensure_api()
    return openai.ChatCompletion.create(
        model=model_name,
        messages=[
            {"role": "system", "content": sytem_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]


def get_tokenizer(encoding) -> Any:
    return tiktoken.get_encoding(encoding)
