# -*- coding: utf-8 -*-

import os
from datetime import datetime
import socket
from pytz import utc

import aword.tools as T

from aword.source.parser import orgmode
from aword.source.parser import markdown
from aword.source.parser import plain

from aword.vector.fields import VectorDbFields

Source = VectorDbFields.SOURCE.value
Source_unit_id = VectorDbFields.SOURCE_UNIT_ID.value
Category = VectorDbFields.CATEGORY.value
Scope = VectorDbFields.SCOPE.value


def add_to_cache(awd, only_in_directory=None):
    supported_extensions = ['org', 'md', 'txt', 'text']

    parsers = {
        'org': orgmode.parse,
        'md': markdown.parse,
        'txt': plain.parse,
        'text': plain.parse,
    }

    all_segments = []
    hostname = socket.gethostname()
    source_name = 'local'
    full_source_name = hostname + ':' + 'local'
    source_unit_cache = awd.get_source_unit_cache()

    # pylint: disable=too-many-nested-blocks
    for source in awd.get_single_source_config(source_name, []):
        directory = source['directory']
        if only_in_directory is not None and directory != only_in_directory:
            continue

        author = source['author']
        category = source.get('category', '')
        scope = source.get('scope', '')

        extensions = source.get('extensions', supported_extensions)
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                file_extension = os.path.splitext(filename)[-1].lower()[1:]
                if file_extension in extensions and file_extension in supported_extensions:
                    file_path = os.path.join(dirpath, filename)

                    # Check the file's last modified time in UTC
                    file_modified_dt = datetime.utcfromtimestamp(
                        os.path.getmtime(file_path)).replace(tzinfo=utc)

                    last_stored_edit_dt = source_unit_cache.get_last_edited_timestamp(
                        full_source_name, file_path)

                    if last_stored_edit_dt is None or file_modified_dt > last_stored_edit_dt:
                        print('  + parsing', file_path)
                        uri = T.file_to_uri(file_path)
                        parser_fn = parsers[file_extension]
                        segments = parser_fn(
                            file_path,
                            uri=uri,
                            author=author,
                            timestamp=file_modified_dt)
                        all_segments += segments

                        source_unit_cache.add_or_update(**{
                            'uri': T.file_to_uri(file_path),
                            Source: full_source_name,
                            Source_unit_id: file_path,
                            Category: category,
                            Scope: scope,
                            'created_by': author,
                            'last_edited_by': author,
                            'last_edited_timestamp': file_modified_dt,
                            'summary': '',
                            'segments': segments,
                            'metadata': {'directory': directory}
                        })
                    else:
                        print('  - ignoring %s with last_stored_edit_dt %s' %
                              (file_path, last_stored_edit_dt))

    return all_segments


def main():
    from aword.app import Awd
    added = add_to_cache(Awd())
    print('Added to cache:')
    print('  ' + '\n  '.join([e.uri for e in added]))


if __name__ == "__main__":
    main()
