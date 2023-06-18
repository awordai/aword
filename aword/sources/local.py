# -*- coding: utf-8 -*-

import os
from datetime import datetime
import socket
from pytz import utc

import aword.tools as T
from aword.payload import FactType
from aword.embed import embed_source_unit

from aword.sources.state import State
from aword.sources import markdown
from aword.sources import orgmode
from aword.sources import plain


def ingest(only_in_directory=None):
    state = State()
    supported_extensions = ["md", "org", "txt", "text"]

    parsers = {
        "md": markdown.parse,
        "org": orgmode.parse,
        "txt": plain.parse,
        "text": plain.parse,
    }

    embedded_payloads = []
    source_name = 'local'
    hostname = socket.gethostname()

    # pylint: disable=too-many-nested-blocks
    for source in T.get_source_config(source_name, []):
        directory = source['directory']
        if only_in_directory is not None and directory != only_in_directory:
            continue

        author = source['author']
        fact_type = FactType(source['fact_type'])

        extensions = source.get('extensions', supported_extensions)
        for dirpath, _, filenames in os.walk(directory):
            for filename in filenames:
                file_extension = os.path.splitext(filename)[-1].lower()[1:]
                if file_extension in extensions and file_extension in supported_extensions:
                    file_path = os.path.join(dirpath, filename)

                    # Check the file's last modified time in UTC
                    file_modified_dt = datetime.utcfromtimestamp(
                        os.path.getmtime(file_path)).replace(tzinfo=utc)

                    last_seen_dt = state.get_last_seen(source_name, file_path)

                    if last_seen_dt is None or file_modified_dt > last_seen_dt:
                        uri = T.file_to_uri(file_path)
                        parser = parsers[file_extension]
                        payloads = parser(
                            file_path,
                            uri=uri,
                            source=source_name + ':' + hostname,
                            author=author,
                            fact_type=fact_type,
                            timestamp=file_modified_dt,
                            metadata={'directory': directory})

                        embed_source_unit(payloads, source_unit_id=file_path)
                        embedded_payloads += payloads

                    state.update_last_seen(source_name, file_path)

    return embedded_payloads


def main():
    embedded = ingest()
    print('Ingested from:')
    print('  ' + '\n  '.join([e.uri for e in embedded]))


if __name__ == "__main__":
    main()
