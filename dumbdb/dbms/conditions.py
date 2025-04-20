from dataclasses import dataclass
from dumbdb.parser.ast import Column


@dataclass
class EqualsCondition:
    column: Column
    value: str


@dataclass
class AndCondition:
    left: 'WhereCondition'
    right: 'WhereCondition'


WhereCondition = EqualsCondition | AndCondition
