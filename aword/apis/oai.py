# -*- coding: utf-8 -*-

import os
from typing import List, Any

import openai
import tiktoken

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
def fetch_embeddings(text_or_tokens_array, model=None) -> List[List[float]]:
    ensure_api()
    return [r['embedding'] for r in
            openai.Embedding.create(input=text_or_tokens_array,
                                    model=model or C['oai_embedding_model'])["data"]]


def get_embeddings(text_chunks: List[str]) -> List[float]:
    # Split text_chunks into shorter arrays of max length 100
    text_chunks_arrays = [text_chunks[i:i+C['oai_max_texts_to_embed_batch_size']]
                          for i in range(0, len(text_chunks),
                                         C['oai_max_texts_to_embed_batch_size'])]

    # Call get_embeddings for each shorter array and combine the results
    embeddings = []
    for text_chunks_array in text_chunks_arrays:
        embeddings += fetch_embeddings(text_chunks_array)

    return embeddings


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


def get_tokenizer() -> Any:
    return tiktoken.get_encoding(C['oai_embedding_encoding'])
