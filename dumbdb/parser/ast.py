from dataclasses import dataclass, field
from typing import Any, List, Optional


class ASTNode:
    """Base class for all AST nodes."""
    pass


@dataclass
class Column(ASTNode):
    name: str


@dataclass
class Table(ASTNode):
    name: str


@dataclass
class Query(ASTNode):
    """Base class for all query nodes."""
    pass


@dataclass
class CreateDatabaseQuery(Query):
    database: str


@dataclass
class ShowDatabasesQuery(Query):
    pass


@dataclass
class DropDatabaseQuery(Query):
    database: str


@dataclass
class UseDatabaseQuery(Query):
    database: str


@dataclass
class CreateTableQuery(Query):
    table: Table
    columns: List[Column]


@dataclass
class ShowTablesQuery(Query):
    pass


@dataclass
class DropTableQuery(Query):
    table: Table


@dataclass
class SelectQuery(Query):
    columns: List[Column]
    table: Table
    where_clause: Optional[Any]


@dataclass
class InsertQuery(Query):
    table: Table
    columns: List[Column]
    values: List[str]
    row: dict[Column, str] = field(init=False)

    def __post_init__(self):
        self.row = dict(zip(self.columns, self.values))
