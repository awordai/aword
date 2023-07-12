from enum import Enum


class FactType(str, Enum):
    reference = 'reference'
    historical = 'historical'
