# -*- coding: utf-8 -*-

import aword.tools as T
from aword.sources.local import orgmode


def test_orgmode_parse():
    file_path = 'res/test/local/org/trees.org'
    payloads = orgmode.parse(file_path,
                             uri=T.file_to_uri(file_path),
                             source='local:hostname',
                             author='Unknown',
                             fact_type='reference',
                             timestamp=T.timestamp_as_utc().isoformat(),
                             metadata={'testing': True})

    assert [payload.headings for payload in payloads] == [
        ['Trees'],
        ['Trees', 'Oak Tree', 'Overview'],
        ['Trees', 'Oak Tree', 'Characteristics'],
        ['Trees', 'Maple Tree', 'Overview'],
        ['Trees', 'Maple Tree', 'Characteristics'],
        ['Trees', 'Pine Tree', 'Overview'],
        ['Trees', 'Pine Tree', 'Characteristics'],
        ['Trees', 'Willow Tree', 'Overview'],
        ['Trees', 'Willow Tree', 'Characteristics']]

    assert [payload.body for payload in payloads] == [
        'Trees are perennial plants with an elongated stem, or trunk, '
        'supporting branches and leaves in most species. They are a vital part '
        'of our ecosystem and provide numerous benefits. This document will '
        'describe a few notable ones.',

        'The Oak tree, belonging to the genus Quercus, '
        'is known for its strength and longevity. There are about 600 species of oaks, '
        'and they are native to the northern hemisphere.',

        'Oak trees are large and deciduous, '
        'though a few tropical species are evergreen. They have spirally arranged '
        'leaves and acorns as fruit. The wood of oak trees is notably strong and '
        'durable.',

        'Maple trees are part of the genus Acer. '
        'They are known for their distinctive leaf shape and the production of '
        'maple syrup.',

        'Maple trees have a diverse range '
        'of properties, with sizes ranging from shrubs to large trees. The leaves '
        'are usually palmately veined and lobed. The fruit is a double samara with '
        'one wing longer than the other.',

        'Pine trees are evergreen, coniferous '
        'resinous trees in the genus Pinus. They are known for their distinctive '
        'pine cones and are often associated with Christmas.',

        'Pine trees can be identified by '
        'their needle-like leaves, which are bundled in clusters of 2-5. '
        'The bark of most pines is thick and scaly, but some species have thin, '
        'flaky bark.',

        'Willow trees, part of the genus Salix, '
        'are known for their flexibility and their association with water and wetlands.',

        'Willow trees are usually '
        'fast-growing but relatively short-lived. They have slender branches and large, '
        'fibrous, often stoloniferous roots. The leaves are typically elongated, '
        'but may also be round to oval.'
    ]
