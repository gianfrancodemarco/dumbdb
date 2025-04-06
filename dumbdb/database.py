from abc import ABC, abstractmethod
from dataclasses import dataclass
from pathlib import Path

from dumbdb.models import QueryResult


@dataclass
class Database(ABC):
    name: str
    root_dir: Path = Path("./data")

    @abstractmethod
    def create_database(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def create_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def drop_table(name: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def insert(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def update(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def delete(table: str, value: dict) -> None:
        raise NotImplementedError()

    @abstractmethod
    def query(table: str, query: dict) -> QueryResult:
        raise NotImplementedError()
