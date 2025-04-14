import csv
from dataclasses import dataclass, field
from io import TextIOWrapper
from time import time

from .append_only_dbms import (AppendOnlyDBMS, QueryResult,
                               require_exists_database, require_exists_table,
                               require_isset_database,
                               require_not_exists_table)
from .hash_index import HashIndex


@dataclass
class AppendOnlyDBMSWithHashIndexes(AppendOnlyDBMS):
    hash_indexes: dict[str, HashIndex] = field(default_factory=dict)

    @require_exists_database
    def use_database(self, db_name: str) -> None:
        super().use_database(db_name)
        for table in self.show_tables():
            self.hash_indexes[table] = HashIndex.from_csv(
                self.get_table_file_path(table), "id")

    @require_isset_database
    @require_not_exists_table
    def create_table(self, table_name: str, headers: list[str] = None) -> None:
        super().create_table(table_name, headers)
        self.hash_indexes[table_name] = HashIndex()

    @require_isset_database
    @require_exists_table
    def drop_table(self, table_name: str) -> None:
        super().drop_table(table_name)
        del self.hash_indexes[table_name]

    @require_isset_database
    @require_exists_table
    def insert(self, table_name: str, row: dict) -> None:
        """Insert a new row into a table."""
        table_file = self.get_table_file_path(table_name)

        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            start_byte = f.tell()
            csv_writer.writerow(list(row.values()) + [False])
            end_byte = f.tell()
            self.hash_indexes[table_name].set_row_offsets(
                row["id"], start_byte, end_byte)

    @require_isset_database
    @require_exists_table
    def update(self, table_name: str, row: dict) -> None:
        # Since the super class implementation of update is just an insert,
        # we only need to implement the index on the insert.
        super().update(table_name, row)

    @require_isset_database
    @require_exists_table
    def delete(self, table_name: str, row: dict) -> None:
        super().delete(table_name, row)
        self.hash_indexes[table_name].delete_row_offsets(row["id"])

    @require_isset_database
    @require_exists_table
    def query(self, table_name: str, query: dict) -> QueryResult:
        """
        If the search is by id, we can use the hash index to find the row.
        Otherwise, we need to search the entire table.
        """
        if "id" in query:
            start_time = time()
            start_byte, end_byte = self.hash_indexes[table_name].get_row_offsets(
                query["id"])

            with open(self.get_table_file_path(table_name), 'rb') as f:
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

    @require_isset_database
    @require_exists_table
    def compact_table(self, table_name: str) -> None:
        super().compact_table(table_name)
        self.hash_indexes[table_name] = HashIndex.from_csv(
            self.get_table_file_path(table_name), "id")
