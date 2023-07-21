# -*- coding: utf-8 -*-


def test_get_oai_embeddings(awd):
    embedder = awd.get_embedder('text-embedding-ada-002')
    embeddings = embedder.get_embeddings(['hola que tal',
                                          'como estas'])
    assert len(embeddings) == 2


def test_get_hf_embeddings(awd):
    embedder = awd.get_embedder('multi-qa-mpnet-base-dot-v1')
    embeddings = embedder.get_embeddings(['hola que tal',
                                          'como estas'])
    assert len(embeddings) == 2


def test_chunks(awd):
    embedder = awd.get_embedder()
    tk, txt = list(zip(*embedder.split_in_chunks('hola que tal. Esto es una prueba de chunk',
                                                 4)))
    assert txt[0] == 'hola que tal.'
    assert embedder.encode(txt[0]) == tk[0]
    assert embedder.encode(txt[-1]) == tk[-1]


def test_oai_embedded_chunks(awd, resdir):
    with open(f'{resdir}/local/org/wands.org', encoding='utf-8') as wands_in:
        with open(f'{resdir}/local/org/trees.org', encoding='utf-8') as trees_in:
            txt = wands_in.read() + trees_in.read()
            oai_embedder = awd.get_embedder('text-embedding-ada-002')
            oai_chunks = oai_embedder.get_embedded_chunks(txt)

            assert oai_chunks[1].payload.body == (
                'Pine trees are evergreen, coniferous resinous trees in the genus '
                'Pinus. They are known for their distinctive pine cones and are '
                'often associated with Christmas.\n'
                '*** Characteristics\n'
                'Pine trees can be identified by their needle-like leaves, which are '
                'bundled in clusters of 2-5. The bark of most pines is thick and '
                'scaly, but some species have thin, flaky bark.\n'
                '** Willow Tree\n'
                '*** Overview\n'
                'Willow trees, part of the genus Salix, are known for their '
                'flexibility and their association with water and wetlands.\n'
                '*** Characteristics\n'
                'Willow trees are usually fast-growing but relatively short-lived. '
                'They have slender branches and large, fibrous, often stoloniferous '
                'roots. The leaves are typically elongated, but may also be round to '
                'oval.\n'
            )
            assert len(oai_chunks[1].vector) == 1536


def test_hf_embedded_chunks(awd, resdir):
    with open(f'{resdir}/local/org/wands.org', encoding='utf-8') as wands_in:
        with open(f'{resdir}/local/org/trees.org', encoding='utf-8') as trees_in:
            txt = wands_in.read() + trees_in.read()
            embedder = awd.get_embedder('multi-qa-mpnet-base-dot-v1')
            chunks = embedder.get_embedded_chunks(txt)

            assert chunks[1].payload.body == (
                '* * pine tree * * * overview pine trees are evergreen, coniferous resinous '
                'trees in the genus pinus. they are known for their distinctive pine cones '
                'and are often associated with christmas. * * * characteristics pine trees '
                'can be identified by their needle - like leaves, which are bundled in '
                'clusters of 2 - 5. the bark of most pines is thick and scaly, but some species '
                'have thin, flaky bark. * * willow tree * * * overview willow trees, part of '
                'the genus salix, are known for their flexibility and their association with '
                'water and wetlands. * * * characteristics willow trees are usually fast - '
                'growing but relatively short - lived. they have slender branches and large, '
                'fibrous, often stoloniferous roots. the leaves are typically elongated, '
                'but may also be round to oval.'
            )
            assert len(chunks[1].vector) == 768
