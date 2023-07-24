# -*- coding: utf-8 -*-


def test_update_cache(awd):
    suc = awd.get_source_unit_cache()
    suc.reset_tables()

    awd.update_cache()

    unembedded = suc.get_unembedded()
    assert len(unembedded) == 14

    assert sum(len(source_unit['segments']) for source_unit in unembedded) == 74


# pylint: disable=unused-argument
def test_embed_and_store(awd, collection_name, ensure_empty_collection):
    awd.embed_and_store(collection_name=collection_name)

    suc = awd.get_source_unit_cache()
    unembedded = suc.get_unembedded()
    assert len(unembedded) == 0
