# -*- coding: utf-8 -*-

import re
import uuid

from qdrant_client.models import PointStruct

import tiktoken

from numpy import array, average

import aword.tools as T
from aword.payload import Chunk, Segment
from aword.apis.oai import get_embeddings
from aword.apis import qdrant


# Split a text into smaller chunks of size n, preferably ending at the
# end of a paragraph or, if no end of paragraph is found, a sentence.
def split_in_chunks(text, n, tokenizer):
    """Yield successive n-sized chunks from text."""
    tokens = tokenizer.encode(re.sub(r'[ \t]+', ' ', text))
    i = 0
    while i < len(tokens):
        # Find the nearest end of paragraph within a range of 0.5 * n and 1.5 * n tokens
        j = min(i + int(1.5 * n), len(tokens))
        while j > i + int(0.5 * n):
            # Decode the tokens and check for full stop or newline
            chunk = tokenizer.decode(tokens[i:j])
            if chunk.endswith("\n"):
                break
            j -= 1

        # If no end of paragraph found, try to find end of sentence
        if j == i + int(0.5 * n):
            j = min(i + int(1.5 * n), len(tokens))
            while j > i + int(0.5 * n):
                # Decode the tokens and check for full stop or newline
                chunk = tokenizer.decode(tokens[i:j])
                if chunk.endswith("."):
                    break
                j -= 1

        # If no end of sentence found, use n tokens as the chunk size
        if j == i + int(0.5 * n):
            j = min(i + n, len(tokens))
        yield tokens[i:j], tokenizer.decode(tokens[i:j])
        i = j


def get_col_average_from_list_of_lists(list_of_lists):
    """Return the average of each column in a list of lists."""
    if len(list_of_lists) == 1:
        return list_of_lists[0]

    list_of_lists_array = array(list_of_lists)
    average_embedding = average(list_of_lists_array, axis=0)
    return average_embedding.tolist()


def create_embeddings(text, include_full_text_if_chunked=False, tokenizer=None):
    """Returns a list of tuples (text_chunk, vector).  If
    include_full_text_if_chunked is True it will add an embedding for
    the full text when it has been chunked.
    """

    C = T.get_config('openai')
    tokenizer = tokenizer or tiktoken.get_encoding(C['oai_embedding_encoding'])

    # Should just return the number of tokens and the text chunks
    token_chunks, text_chunks = list(zip(*split_in_chunks(re.sub('\n+','\n', text),
                                                          C['oai_embedding_chunk_size'],
                                                          tokenizer)))

    # Split text_chunks into shorter arrays of max length 100
    text_chunks_arrays = [text_chunks[i:i+C['oai_max_texts_to_embed_batch_size']]
                          for i in range(0, len(text_chunks),
                                         C['oai_max_texts_to_embed_batch_size'])]

    # Call get_embeddings for each shorter array and combine the results
    embeddings = []
    for text_chunks_array in text_chunks_arrays:
        embeddings += get_embeddings(text_chunks_array)

    chunks = [Chunk(text=t,
                    vector=e)
              for t, e in (zip(text_chunks, embeddings))]

    if include_full_text_if_chunked and len(chunks) > 1:
        total_tokens = sum(len(chunk) for chunk in token_chunks)
        if total_tokens >= C['oai_embedding_ctx_length']:
            average_embedding = get_col_average_from_list_of_lists(embeddings)
        else:
            average_embedding = get_embeddings(['\n\n'.join(text_chunks)])[0]
        chunks.append(Chunk(text=text,
                            vector=average_embedding))

    return chunks


def make_id(source_unit_id, text):
    return str(uuid.uuid5(uuid.NAMESPACE_X500, source_unit_id + text))


def embed_source_unit(segments, source_unit_id=None):
    """The source_unit_id is the id of the atomic source unit. If the
    source is notion, for example, the atomic source unit is the
    document, and the source_unit_id would be the document id. If the
    source is local the source unit is the file (in most cases) and
    the source unit id will be the file name.
    """
    q_client = qdrant.get_qdrant_client()
    C = T.get_config('qdrant')

    if source_unit_id is not None:
        qdrant.clean_source_unit(source_unit_id)

    points = []

    # We don't want to mutate the incoming segments
    out_segments = []

    print('embedding', source_unit_id)
    for segment in segments:
        out_segment = Segment.copy(segment)
        out_segment.chunks = []

        # TODO Maybe it should include the headings in the embedding
        for chunk in create_embeddings(segment.body):
            payload = Segment.copy(segment)
            payload.body = chunk.text
            chunk.vector_db_id = make_id(source_unit_id or payload.source_unit_id,
                                         chunk.text)

            points.append(PointStruct(id=chunk.vector_db_id,
                                      vector=chunk.vector,
                                      payload=payload))
            out_segment.chunks.append(chunk)

        out_segments.append(out_segment)

    print('... about to upsert', len(points), 'points for', source_unit_id)
    q_client.upsert(collection_name=C['qdrant_collection'],
                    points=points)

    return out_segments
