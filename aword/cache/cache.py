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
    parser.add_argument('--source-unit-list-from-source',
                        help='Pretty-print all the rows from a source in the source unit cache',
                        type=str)
    parser.add_argument('--source-unit-reset-last-embedded',
                        help='Set the last embedded datetime to None, forcing the next embed',
                        action='store_true')
    parser.add_argument('--chunk-list-from-source',
                        help='Pretty-print all the rows from a source in the chunk cache',
                        type=str)
    parser.add_argument('--chunk-list-from-source-unit',
                        help=('Pretty-print all the rows from a source,source_unit '
                              'in the chunk cache'),
                        type=str)
    parser.add_argument('--chunk-list-all',
                        help='Pretty-print all the rows in the chunk cache',
                        action='store_true')
    parser.add_argument('--chunk-reset-table',
                        help='Drop and recreate the chunk table',
                        action='store_true')


# pylint: disable=too-many-branches
def main(awd, args):
    from pprint import pprint
    suc = awd.get_source_unit_cache()
    if args['source_unit_list_from_source']:
        for row in suc.get_by_source(source=args['source_units_from_source']):
            pprint(row)

    if args['source_unit_reset_last_embedded']:
        suc.reset_embedded()

    cuc = awd.get_chunk_cache()
    if args['chunk_list_from_source']:
        for row in cuc.get_by_source(source=args['chunks_from_source']):
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)

    cuc = awd.get_chunk_cache()
    if args['chunk_list_from_source_unit']:
        source, source_unit_id = args['chunks_from_source_unit'].split(',')
        for row in cuc.get_by_source_unit(source=source, source_unit_id=source_unit_id):
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)

    if args['chunk_list_all']:
        for row in cuc.get_all():
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)

    if args['chunk_reset_table']:
        cuc.reset_table(only_in_memory=False)
