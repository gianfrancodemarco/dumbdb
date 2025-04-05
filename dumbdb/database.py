from abc import ABC, abstractmethod

from dumbdb.models import QueryResult, Table


class Database(ABC):

    @abstractmethod
    def create_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def drop_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def insert(table: Table, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(table: Table, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(table: Table, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def query(table: Table, query: dict) -> QueryResult:
        raise NotImplementedError()
