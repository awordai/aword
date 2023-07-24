# -*- coding: utf-8 -*-
"""Query the cache.
"""
from abc import ABC, abstractmethod
import langdetect


def combine_segments(segments):
    return '\n\n'.join([str(segment) for segment in segments])


def guess_language(source_unit_text, default=''):
    try:
        return langdetect.detect(source_unit_text)
    except:
        return default


class Cache(ABC):

    def __init__(self, summarizer):
        self.summarizer = summarizer

    def summarize(self, text):
        if self.summarizer is not None:
            if len(text.split()) > self.summarizer.get_param('summary_words'):
                summary = self.summarizer.ask(text)
                if summary['success']:
                    return summary['with_arguments']['summary']
                return text[:self.summarizer.get_param('summary_words')] + '...'
            return text
        return ''

    @abstractmethod
    def get_last_edited_timestamp(self, source, source_unit_id):
        """
        This method should be implemented by any class that extends Cache.
        It should return the last seen timestamp for the provided source and source_unit_id.
        """


def add_args(parser):
    parser.add_argument('--source-units-from-source',
                        help='Pretty-print all the rows from a source in the source unit cache',
                        type=str)
    parser.add_argument('--chunks-from-source',
                        help='Pretty-print all the rows from a source in the chunk cache',
                        type=str)
    parser.add_argument('--all-chunks',
                        help='Pretty-print all the rows in the chunk cache',
                        action='store_true')


def main(awd, args):
    from pprint import pprint
    suc = awd.get_source_unit_cache()
    if args['source_units_from_source']:
        for row in suc.get_by_source(source=args['source_units_from_source']):
            pprint(row)

    cuc = awd.get_chunk_cache()
    if args['chunks_from_source']:
        for row in cuc.get_by_source(source=args['chunks_from_source']):
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)

    if args['all_chunks']:
        for row in cuc.get_all():
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)
