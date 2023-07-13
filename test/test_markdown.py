# -*- coding: utf-8 -*-

import aword.tools as T
from aword.source.local import markdown


def test_parse(resdir):
    file_path = f'{resdir}/local/butterfly-biology.md'
    payloads = markdown.parse(file_path,
                              uri=T.file_to_uri(file_path),
                              author='Unknown',
                              timestamp=T.timestamp_as_utc().isoformat(),
                              metadata={'testing': True})

    assert payloads[0].headings == ['Butterflies', 'Anatomy of a Butterfly', 'Wings']
    assert payloads[0].body == (
        'Butterflies are known for their large, colorful wings. '
        'The wings are made up of thin layers of chitin, the same '
        'protein that makes up the exoskeleton of a butterfly. '
        'The colors of the wings are created by the reflection and '
        'refraction of light by the microscopic scales that cover them.')

    assert payloads[1].headings == ['Butterflies', 'Anatomy of a Butterfly', 'Body']
    assert payloads[1].body == (
        'The body of a butterfly is divided into three parts: the head, '
        'the thorax, and the abdomen. The head houses the eyes, antennae, '
        'and proboscis. The thorax contains the muscles that control the wings. '
        'The abdomen contains the digestive and reproductive organs.')

    assert payloads[2].headings == ['Butterflies', 'Life Cycle of a Butterfly']
    assert payloads[2].body == (
        'The butterfly has a most interesting life cycle.')

    assert payloads[3].headings == ['Butterflies',
                                    'Life Cycle of a Butterfly',
                                    'Caterpillar Stage']
    assert payloads[3].body == (
        'The life cycle of a butterfly begins as an egg. From the egg hatches '
        'the caterpillar, or larva. The caterpillar spends most of its time eating, '
        'growing rapidly and shedding its skin several times.')

    assert payloads[4].headings == ['Butterflies',
                                    'Life Cycle of a Butterfly',
                                    'Chrysalis Stage']
    assert payloads[4].body == (
        'Once the caterpillar has grown enough, it forms a chrysalis, or pupa. '
        'Inside the chrysalis, the caterpillar undergoes a transformation '
        'called metamorphosis. Over the course of a few weeks, it reorganizes '
        'its body and emerges as a butterfly.')
