# -*- coding: utf-8 -*-


def test_update_cache(awd):
    suc = awd.get_source_unit_cache()
    suc.reset_tables()

    awd.update_cache()

    unembedded = suc.list_unembedded_rows()
    assert len(unembedded) == 15

    assert sum(len(source_unit['segments']) for source_unit in unembedded) == 62


# pylint: disable=unused-argument
def test_embed_and_store(awd, ensure_empty_vector_namespace):
    awd.embed_and_store()

    suc = awd.get_source_unit_cache()
    unembedded = suc.list_unembedded_rows()
    assert len(unembedded) == 0
