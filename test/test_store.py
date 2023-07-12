# -*- coding: utf-8 -*-


from aword.segment import Segment


def test_store(awd, collection_name):
    store = awd.get_store(collection_name)

    store.client.delete_collection(collection_name)
    awd.create_store_collection(collection_name)

    embedder = awd.get_embedder()

    resdir = 'test/res'

    with open(f'{resdir}/local/org/wands.org', encoding='utf-8') as wands_in:
        with open(f'{resdir}/local/org/trees.org', encoding='utf-8') as trees_in:
            txt = wands_in.read() + trees_in.read()

            # txt = trees_in.read()
            segment = Segment(txt,
                              uri='file://uri_to_file_path',
                              headings=['Wands', 'Wand composition'])

            store.store_source_unit(embedder=embedder,
                                    source='source',
                                    source_unit_id='1',
                                    category='reference',
                                    scope='confidential',
                                    segments=[segment])

            assert store.count() == 2

            # If we embed again the same source_unit_id it should first delete the
            # previous version
            store.store_source_unit(embedder=embedder,
                                    source='source',
                                    source_unit_id='1',
                                    category='reference',
                                    scope='confidential',
                                    segments=[segment])
            assert store.count() == 2

            # But if we embed it with a different source_unit_id it should make new
            # points
            chunks = store.store_source_unit(embedder=embedder,
                                             source='source',
                                             source_unit_id='2',
                                             category='reference',
                                             scope='confidential',
                                             segments=[segment])

            assert len(chunks) == 2
            assert store.count() == 4
