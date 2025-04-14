from dataclasses import dataclass, field
import traceback

from dumbdb.dbms.append_only_dbms_with_hash_indexes import \
    AppendOnlyDBMSWithHashIndexes
from dumbdb.dbms.dbms import DBMS
from dumbdb.parser.ast import InsertQuery, Query, SelectQuery
from dumbdb.parser.parser import Parser
from dumbdb.parser.tokenizer import Tokenizer


class QueryResult:
    def __init__(self, rows: list[dict]):
        self.rows = rows


@dataclass
class Executor:
    dbms: DBMS

    def execute_query(self, query: Query) -> QueryResult:
        operations = {
            SelectQuery: self.execute_select_query,
            InsertQuery: self.execute_insert_query,
        }

        if type(query) not in operations:
            raise Exception(f"Query type {type(query)} not supported.")

        return operations[type(query)](query)

    def execute_select_query(self, query: SelectQuery) -> QueryResult:
        return self.dbms.query(query.table.name, {})

    def execute_insert_query(self, query: InsertQuery) -> QueryResult:
        self.dbms.insert(query.table.name, query.row)
        return QueryResult(rows=[])


@dataclass
class DBEngine:
    dbms: DBMS = field(default_factory=AppendOnlyDBMSWithHashIndexes)
    parser: Parser = field(default_factory=Parser)
    executor: Executor = field(init=False)

    def __post_init__(self):
        self.executor = Executor(self.dbms)

    def cli(self):
        """
        Command line interface for the DBEngine.
        """
        while True:
            try:
                command = input("> ")
                if command.lower() == "exit":
                    break
                result = self.execute_query(command)
                print(result)
            except Exception as e:
                print(f"Error: {str(e)}")
                print("Stack trace:")
                traceback.print_exc()

    def execute_query(self, query: str) -> QueryResult:
        """
        Execute a query passed in as a string.
        1) Tokenize the query.
        2) Parse the tokens into an AST.
        3) Execute the AST.
        """
        tokens = Tokenizer().tokenize(query)
        ast = self.parser.parse(tokens)
        if ast is not None:
            return self.executor.execute_query(ast)
        else:
            raise Exception("Invalid query.")


# if __name__ == "__main__":
#     dbms = AppendOnlyDBMSWithHashIndexes()
#     dbms.use_database("testdb")
#     dbms.insert("my_table", {"id": 1, "name": "Alice"})
#     dbms.insert("my_table", {"id": 2, "name": "Bob"})
#     db_engine = DBEngine(dbms)
#     result = db_engine.execute_query("SELECT * FROM my_table;")
#     print(result)
