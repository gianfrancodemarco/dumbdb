import csv
from dataclasses import dataclass, field
from io import TextIOWrapper
from time import time

from .append_only_dbms import AppendOnlyDBMS, require_isset_database
from .hash_index import HashIndex
from .models import QueryResult


@dataclass
class AppendOnlyDBMSWithHashIndexes(AppendOnlyDBMS):
    hash_indexes: dict[str, HashIndex] = field(default_factory=dict)

    def use_database(self, name: str) -> None:
        super().use_database(name)
        for table in self.get_tables(name):
            self.hash_indexes[table] = HashIndex.from_csv(
                self.get_table_file_path(table), "id")

    @require_isset_database
    def create_table(self, name: str, headers: list[str] = None) -> None:
        super().create_table(name, headers)
        self.hash_indexes[name] = HashIndex()

    @require_isset_database
    def drop_table(self, name: str) -> None:
        super().drop_table(name)
        del self.hash_indexes[name]

    @require_isset_database
    def insert(self, table_name: str, row: dict) -> None:
        """Insert a new row into a table."""
        table_file = self.get_table_file_path(table_name)
        if not table_file.exists():
            raise ValueError(f"Table '{table_name}' does not exist")

        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            start_byte = f.tell()
            csv_writer.writerow(list(row.values()) + [False])
            end_byte = f.tell()
            self.hash_indexes[table_name].set_row_offsets(
                row["id"], start_byte, end_byte)

    @require_isset_database
    def update(self, table_name: str, row: dict) -> None:
        # Since the super class implementation of update is just an insert,
        # we only need to implement the index on the insert.
        super().update(table_name, row)

    @require_isset_database
    def delete(self, table_name: str, row: dict) -> None:
        super().delete(table_name, row)
        self.hash_indexes[table_name].delete(row["id"])

    @require_isset_database
    def query(self, table_name: str, query: dict) -> QueryResult:
        """
        If the search is by id, we can use the hash index to find the row.
        Otherwise, we need to search the entire table.
        """
        if "id" in query:
            start_time = time()
            table_file = self.get_table_file_path(table_name)
            if not table_file.exists():
                raise ValueError(f"Table '{table_name}' does not exist")

            start_byte, end_byte = self.hash_indexes[table_name].get_row_offsets(
                query["id"])
            with open(table_file, 'rb') as f:
                f = TextIOWrapper(f, encoding='utf-8', newline='')

                # Read the header row and store it.
                header_line = f.readline()
                headers = next(csv.reader([header_line]))

                f.seek(start_byte)
                row = f.read(end_byte - start_byte)
                if row:
                    row_values = row.strip().split(",")
                    row_dict = dict(zip(headers, row_values))
                    row_dict.pop("__deleted__")
                    return QueryResult(time() - start_time, [row_dict])

        return super().query(table_name, query)
