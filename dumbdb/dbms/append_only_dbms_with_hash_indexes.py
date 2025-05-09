import csv
from dataclasses import dataclass, field
from io import TextIOWrapper
from time import time

from dumbdb.parser.ast import EqualsCondition, WhereCondition

from .append_only_dbms import (AppendOnlyDBMS, QueryResult,
                               require_exists_database, require_exists_table,
                               require_isset_database,
                               require_not_exists_table)
from .hash_index import HashIndex


@dataclass
class AppendOnlyDBMSWithHashIndexes(AppendOnlyDBMS):
    hash_indexes: dict[str, HashIndex] = field(default_factory=dict)

    @require_exists_database
    def use_database(self, db_name: str) -> QueryResult:
        super().use_database(db_name)
        for table in self.show_tables().rows:
            self.hash_indexes[table] = HashIndex.from_csv(
                self.get_table_file_path(table), "id")
        return QueryResult()

    @require_isset_database
    @require_not_exists_table
    def create_table(self, table_name: str, headers: list[str] = None) -> QueryResult:
        super().create_table(table_name, headers)
        self.hash_indexes[table_name] = HashIndex()
        return QueryResult()

    @require_isset_database
    @require_exists_table
    def drop_table(self, table_name: str) -> QueryResult:
        super().drop_table(table_name)
        del self.hash_indexes[table_name]
        return QueryResult()

    @require_isset_database
    @require_exists_table
    def insert(self, table_name: str, row: dict) -> QueryResult:
        """Insert a new row into a table."""
        table_file = self.get_table_file_path(table_name)

        with open(table_file, 'a', newline='') as f:
            csv_writer = csv.writer(f)
            start_byte = f.tell()
            csv_writer.writerow(list(row.values()) + [False])
            end_byte = f.tell()
            self.hash_indexes[table_name].set_row_offsets(
                row["id"], start_byte, end_byte)

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def update(self, table_name: str, set_clause: dict, where_clause: WhereCondition = None) -> QueryResult:
        # Since the super class implementation of update is just an insert,
        # we only need to implement the index on the insert.
        return super().update(table_name, set_clause, where_clause)

    @require_isset_database
    @require_exists_table
    def delete(self, table_name: str, where_clause: WhereCondition = None) -> QueryResult:
        """
        Delete rows from a table that match the where clause.
        We need to update the hash index after the deletion.
        """
        # First find all rows that will be deleted
        matching_rows = self.query(table_name, where_clause).rows

        # Update the hash index
        for row in matching_rows:
            super().delete(table_name, where_clause)
            self.hash_indexes[table_name].delete_row_offsets(row["id"])

        return QueryResult()

    @require_isset_database
    @require_exists_table
    def query(self, table_name: str, where_clause: WhereCondition = None) -> QueryResult:
        """
        If the search is by id, we can use the hash index to find the row.
        Otherwise, we need to search the entire table.
        """
        if isinstance(where_clause, EqualsCondition) and where_clause.column.name == "id":
            start_time = time()

            try:
                start_byte, end_byte = self.hash_indexes[table_name].get_row_offsets(
                    where_clause.value)
            except KeyError:
                return QueryResult(time=time() - start_time, rows=[])

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

                    # Apply WHERE clause if present
                    if where_clause is not None:
                        if not self.evaluate_where_clause(row_dict, where_clause):
                            return QueryResult(time=time() - start_time, rows=[])

                    return QueryResult(
                        time=time() - start_time,
                        rows=[row_dict]
                    )

        return super().query(table_name, where_clause)

    @require_isset_database
    @require_exists_table
    def compact_table(self, table_name: str) -> QueryResult:
        super().compact_table(table_name)
        self.hash_indexes[table_name] = HashIndex.from_csv(
            self.get_table_file_path(table_name), "id")
        return QueryResult()
