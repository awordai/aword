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
    parser.add_argument('--source',
                        help='Limit listings and actions to this source.',
                        type=str)
    parser.add_argument('--source-unit-id',
                        help=('Limit listings and actions to this source unit id. '
                              'It only makes sense if source is also defined with --source.'),
                        type=str)

    parser.add_argument('--list-source-units',
                        help=('Pretty-print the rows in the source unit cache, '
                              'possibly restricted to a single source if one is specified.'),
                        action='store_true')
    parser.add_argument('--list-chunks',
                        help=('Pretty-print the rows in the chunk cache, '
                              'possibly restricted to a source [, source unit].'),
                        action='store_true')
    parser.add_argument('--reset-embedded',
                        help=('Set the last embedded datetime to None, forcing the next embed, '
                              'possibly restricted to a source [, source unit].'),
                        action='store_true')
    parser.add_argument('--chunk-reset-table',
                        help='Drop and recreate the chunk table',
                        action='store_true')


def main(awd, args):
    from pprint import pprint

    source = args['source']
    source_unit_id = args['source_unit_id']

    if source_unit_id and not source:
        awd.logger.error('Got source-unit-id but no source, please specify one.')
        import sys
        sys.exit(1)

    source_unit_cache = awd.get_source_unit_cache()
    chunk_cache = awd.get_chunk_cache()

    if args['list_source_units']:
        for row in source_unit_cache.list_rows(source=source):
            pprint(row)

    if args['list_chunks']:
        for row in chunk_cache.list_rows(source=source,
                                         source_unit_id=source_unit_id):
            row_copy = row.copy()
            if row_copy['vector']:
                row_copy['vector'] = [row_copy['vector'][0], '...', row_copy['vector'][-1]]
            pprint(row_copy)

    if args['reset_embedded']:
        source_unit_cache.reset_embedded(source=source,
                                         source_unit_id=source_unit_id)

    if args['chunk_reset_table']:
        chunk_cache.reset_table(only_in_memory=False)
