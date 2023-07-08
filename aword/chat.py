# -*- coding: utf-8 -*-

from enum import Enum

import aword.tools as T
from aword.apis import oai, qdrant


def format_context(payload_dict):
    ctx_list = ['```']

    uri = payload_dict.get('uri', '')
    if uri:
        ctx_list.append('url_or_file: ' +
                        (T.uri_to_file_path(uri) if uri.startswith('file:')
                         else uri))

    for field in ('source', 'created_by', 'edited_by', 'fact_type', 'timestamp'):
        value = payload_dict.get(field, '')
        if value:
            ctx_list.append(field + ': ' + (value.value if isinstance(value, Enum)
                                            else str(value)))

    headings = payload_dict.get('headings', [])
    if headings:
        ctx_list.append('breadcrumbs: ' + ' > '.join(headings))

    body = payload_dict.get('body', '')
    if body:
        ctx_list.append('body: ' + body)

    ctx_list.append('```')

    return '\n'.join(ctx_list)


def process_question(question):
    C = T.get_config('openai')

    with open(C['oai_system_prompt'], encoding='utf-8') as fin:
        system_prompt = fin.read()

    all_context = qdrant.search(oai.get_embeddings([question])[0],
                                limit=C['oai_n_chunks_for_context'])

    user_prompt = '\n\n'.join(['# Context:',
                               '\n'.join([format_context(ctx) for ctx in all_context]),
                               '# Question',
                               question])

    answer = oai.ask_question(system_prompt, user_prompt)
    return answer


def main():
    import sys
    question = sys.argv[1]
    print(process_question(question))


if __name__ == '__main__':
    main()
