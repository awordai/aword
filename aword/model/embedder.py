# -*- coding: utf-8 -*-

import re
from typing import Callable, Any, List, Dict

import numpy as np

from aword.chunk import Payload, Chunk
from aword.apis import oai


def make_embedder(awd, config: Dict):
    provider = config.get('provider')

    if provider == 'huggingface':
        return HuggingFaceEmbedder(awd, **config)

    if provider == 'openai':
        return OAIEmbedder(awd, **config)

    raise ValueError(f"Unknown model provider '{provider}'")


def get_col_average_from_list_of_lists(list_of_lists: List[List]):
    """Return the average of each column in a list of lists."""
    if len(list_of_lists) == 1:
        return list_of_lists[0]

    list_of_lists_array = np.array(list_of_lists)
    average_embedding = np.average(list_of_lists_array, axis=0)
    return average_embedding.tolist()


class Embedder:

    def __init__(self,
                 tokenizer: Any,
                 embedding_fn: Callable,
                 chunk_size: int,
                 model_name: str,
                 max_sequence_length: int,
                 dimensions: int):
        self.tokenizer = tokenizer
        self.embedding_fn = embedding_fn

        # TODO Suppoprt larger chunk sizes, and do an average of the vectors.
        assert chunk_size <= max_sequence_length

        self.chunk_size = chunk_size
        self.model_name = model_name
        self.max_sequence_length = max_sequence_length
        self.dimensions = dimensions

    def encode(self, text):
        return self.tokenizer.encode(text)

    def decode(self, text):
        return self.tokenizer.decode(text)

    def split_in_chunks(self,
                        text: str,
                        n: int) -> str:
        """Split a text into smaller chunks of size n, preferably ending at the
        end of a paragraph or, if no end of paragraph is found, a sentence.

        Yield successive n-sized (chunk_tokens, chunk_text) from text."""

        #! tokenizer is an object with an encode and a decode
        tokens = self.tokenizer.encode(re.sub(r'[ \t]+', ' ', text))
        i = 0
        while i < len(tokens):
            # Find the nearest end of paragraph within a range of 0.5 * n and 1.5 * n tokens
            j = min(i + int(1.5 * n), len(tokens))
            while j > i + int(0.5 * n):
                # Decode the tokens and check for full stop or newline
                chunk = self.tokenizer.decode(tokens[i:j])
                if chunk.endswith("\n"):
                    break
                j -= 1

            # If no end of paragraph found, try to find end of sentence
            if j == i + int(0.5 * n):
                j = min(i + int(1.5 * n), len(tokens))
                while j > i + int(0.5 * n):
                    # Decode the tokens and check for full stop or newline
                    chunk = self.tokenizer.decode(tokens[i:j])
                    if chunk.endswith("."):
                        break
                    j -= 1

            # If no end of sentence found, use n tokens as the chunk size
            if j == i + int(0.5 * n):
                j = min(i + n, len(tokens))
            yield tokens[i:j], self.tokenizer.decode(tokens[i:j])
            i = j

    def get_embeddings(self,
                       chunked_texts: List[str]) -> List[float]:
        return self.embedding_fn(chunked_texts, self.model_name)

    def get_embedded_chunks(self,
                            text: str,
                            include_full_text_if_chunked: bool = False) -> List[Chunk]:
        """Returns a list of Chunk objects.  If include_full_text_if_chunked is
        True it will add an embedding for the full text when it has been
        chunked.

        - include_full_text_if_chunked: adds a chunk with all the text.
        """

        # Should just return the number of tokens and the text chunks
        chunked_tokens, chunked_texts = list(zip(*self.split_in_chunks(re.sub('\n+','\n', text),
                                                                       self.chunk_size)))

        embeddings = self.get_embeddings(chunked_texts)
        chunks = [Chunk(payload=Payload(body=t),
                        vector=e)
                  for t, e in (zip(chunked_texts, embeddings))]

        if include_full_text_if_chunked and len(chunks) > 1:
            total_tokens = sum(len(chunk) for chunk in chunked_tokens)
            if total_tokens >= self.max_sequence_length:
                average_embedding = get_col_average_from_list_of_lists(embeddings)
            else:
                average_embedding = self.get_embeddings(['\n\n'.join(chunked_texts)])[0]
            chunks.append(Chunk(payload=Payload(body=text),
                                vector=average_embedding))

        return chunks


class OAIEmbedder(Embedder):

    def __init__(self,
                 awd,
                 embedding_chunk_size: int,
                 model_name: str = 'text-embedding-ada-002',
                 encoding: str = 'cl100k_base',
                 max_sequence_length: int = 8191,
                 dimensions: int = 1536,
                 **_):
        oai.ensure_api(awd.getenv('OPENAI_API_KEY'))
        super().__init__(tokenizer=oai.get_tokenizer(encoding),
                         embedding_fn=oai.get_embeddings,
                         chunk_size=embedding_chunk_size,
                         model_name=model_name,
                         max_sequence_length=max_sequence_length,
                         dimensions=dimensions)


class HuggingFaceEmbedder(Embedder):

    def __init__(self,
                 awd,
                 embedding_chunk_size: int,
                 model_name: str = 'multi-qa-mpnet-base-dot-v1',
                 max_sequence_length: int = 512,
                 dimensions: int = 768,
                 **_):

        # Imported here because it is a rather expensive import
        from sentence_transformers import SentenceTransformer

        # The HuggingFace tokenizer
        # https://huggingface.co/transformers/v4.4.2/_modules/transformers/models/mpnet/tokenization_mpnet_fast.html
        # adds bos_token="<s>" and eos_token="</s>" at tthe beginning and at
        # the end of the text when calling encode. This messes up the chunking.

        class _Tokenizer:

            def __init__(self, tokenizer):
                self.tokenizer = tokenizer

            def encode(self, txt):
                return self.tokenizer.encode(txt)[1:-1]

            def decode(self, token_array):
                return self.tokenizer.decode(token_array)

        model = SentenceTransformer(model_name)

        # It is called with model_name as the second argument
        def _get_embeddings(chunked_texts, _):
            return model.encode(chunked_texts).tolist()

        super().__init__(tokenizer=_Tokenizer(model.tokenizer),
                         embedding_fn=_get_embeddings,
                         chunk_size=embedding_chunk_size,
                         model_name=model_name,
                         max_sequence_length=max_sequence_length,
                         dimensions=dimensions)
