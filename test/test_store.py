# -*- coding: utf-8 -*-


from aword.segment import Segment


def test_store(awd, collection_name, resdir):
    store = awd.get_store(collection_name)

    store.client.delete_collection(collection_name)
    awd.create_store_collection(collection_name)

    embedder = awd.get_embedder()

    with open(f'{resdir}/local/org/wands.org', encoding='utf-8') as wands_in:
        with open(f'{resdir}/local/org/trees.org', encoding='utf-8') as trees_in:
            txt = wands_in.read() + trees_in.read()

            categories_1 = ['wedding', 'gift']
            scope_1 = 'confidential'
            context_1 = 'internal_comm'
            source_1_2 = 'source'
            headings_1 = ['Wands', 'Wand composition']
            segment_1 = Segment(txt,
                                uri='file://uri_to_file_path',
                                headings=headings_1)

            store.store_source_unit(embedder=embedder,
                                    source=source_1_2,
                                    source_unit_id='1',
                                    categories=categories_1,
                                    scope=scope_1,
                                    context=context_1,
                                    language='en',
                                    segments=[segment_1])

            assert store.count() == 2

            # If we embed again the same source_unit_id it should first delete the
            # previous version
            store.store_source_unit(embedder=embedder,
                                    source=source_1_2,
                                    source_unit_id='1',
                                    categories=categories_1,
                                    scope=scope_1,
                                    context=context_1,
                                    language='en',
                                    segments=[segment_1])
            assert store.count() == 2

            # But if we embed it with a different source_unit_id it should make new
            # points
            categories_2 = ['wedding', 'present']
            scope_support = 'support'
            context_2 = 'customer_comm'
            headings_2 = ['Wands and trees', 'Wand composition plus trees']
            segment_2 = Segment(txt,
                                uri='file://uri_to_file_path',
                                headings=headings_2)

            chunks = store.store_source_unit(embedder=embedder,
                                             source=source_1_2,
                                             source_unit_id='2',
                                             categories=categories_2,
                                             scope=scope_support,
                                             context=context_2,
                                             language='en',
                                             segments=[segment_2])

            assert len(chunks) == 2
            assert store.count() == 4

            # Add another chunk with a different source
            source_3 = 'source 3'
            segment_2 = Segment('A very short text',
                                uri='file://uri_to_file_path',
                                headings=headings_2)
            categories_3 = ['wedding', 'regalo']

            chunks = store.store_source_unit(embedder=embedder,
                                             source=source_3,
                                             source_unit_id='3',
                                             categories=categories_3,
                                             scope=scope_support,
                                             context=context_2,
                                             language='en',
                                             segments=[segment_2])
            assert store.count() == 5
            assert len(store.fetch_all()) == 5

            assert len(store.fetch_all(sources=source_1_2)) == 4

            support_chunks = store.fetch_all(scopes=scope_support)
            assert len(support_chunks) == 3
            assert all(chunk['scope'] == scope_support for chunk in support_chunks)

            assert len(store.fetch_all(categories='wedding')) == 5
            assert len(store.fetch_all(categories='present')) == 2
            assert len(store.fetch_all(categories=['present', 'regalo'])) == 3
