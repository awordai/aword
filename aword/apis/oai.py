# -*- coding: utf-8 -*-

import os
import json
from typing import List, Any, Dict

import openai
import tiktoken

from tenacity import retry, wait_random_exponential
from tenacity import stop_after_attempt, retry_if_not_exception_type

import aword.tools as T
import aword.errors as E


GPT_MODEL = "gpt-3.5-turbo-0613"
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
def chat(model_name: str,
         system_prompt: str,
         user_prompt: str):
    ensure_api()
    return openai.ChatCompletion.create(
        model=model_name,
        messages=[
            {"role": "system", "content": system_prompt},
            {"role": "user", "content": user_prompt},
        ],
    )["choices"][0]["message"]["content"]


@retry(wait=wait_random_exponential(min=1, max=20),
       stop=stop_after_attempt(6),
       retry=retry_if_not_exception_type(E.AwordError))
def chat_completion_request(messages: List[Dict],
                            functions: List[Dict] = None,
                            call_function: str = None,
                            temperature: float = 1,  # 0 to 2
                            model_name: str = GPT_MODEL) -> Dict:
    ensure_api()

    args = {'model': model_name,
            'messages': messages,
            'temperature': temperature}

    if functions:
        args['functions'] = functions

    if call_function:
        if not functions:
            raise E.AwordError(f'Cannot call function {call_function} if no functions are defined')

        found_function = len([fdesk for fdesk in functions if fdesk['name'] == call_function]) == 1
        if not found_function:
            raise E.AwordError(f'Cannot call undefined function {call_function}')
        if found_function > 1:
            raise E.AwordError(f'Found more than one definitions of {call_function}')

        args['function_call'] = {'name': call_function}

    try:
        response = openai.ChatCompletion.create(**args)["choices"][0]["message"]
        function_call = response.get('function_call', None)
        if function_call:
            return {'call_function': function_call['name'],
                    'with_arguments': json.loads(function_call['arguments'])}
        return {'reply': response['content']}
    except openai.InvalidRequestError as exc:
        raise E.AwordError('Invalid request error from OpenAI') from exc


def get_tokenizer(encoding) -> Any:
    return tiktoken.get_encoding(encoding)
