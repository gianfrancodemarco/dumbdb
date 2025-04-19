from pathlib import Path

import matplotlib.pyplot as plt

from dumbdb.dbms import AppendOnlyDBMS, AppendOnlyDBMSWithHashIndexes

# Global constants for dataset sizes to test
DATASET_SIZES = [100, 500, 1000, 2500, 5000, 10000]
# 20000, 30000, 40000, 50000, 75000, 100000]

# Number of iterations for calculating average times
NUM_ITERATIONS = 1


def setup_database(temp_dir, with_indexes=False):
    """Helper function to set up a single database for testing"""
    # Create directory for the DBMS implementation
    db_dir = Path(temp_dir) / ("with_indexes" if with_indexes else "regular")

    # Initialize DBMS
    if with_indexes:
        dbms = AppendOnlyDBMSWithHashIndexes(root_dir=db_dir)
    else:
        dbms = AppendOnlyDBMS(root_dir=db_dir)

    # Create database and table
    dbms.create_database("test_db")
    dbms.use_database("test_db")
    dbms.create_table("users", ["id", "name", "age", "email", "city"])

    return db_dir, dbms


def generate_user_data(i):
    """Helper function to generate consistent user data"""
    return {
        "id": str(i),
        "name": f"User {i}",
        "age": str(20 + (i % 50)),
        "email": f"user{i}@example.com",
        "city": f"City {i % 10}"
    }


def populate_database(dbms, size):
    """Helper function to populate a database with a specific number of records"""
    for i in range(size):
        user = generate_user_data(i)
        dbms.insert("users", user)


def plot_results(
    title: str,
    x_axis_data: list[int],
    x_axis_label: str,
    y_axis: list[dict[str, float]],
    y_axis_label: str,
):

    plt.figure(figsize=(10, 5))
    for y_axis_data in y_axis:
        plt.plot(x_axis_data, y_axis_data['values'],
                 label=y_axis_data['label'])

    plt.title(title)
    plt.xlabel(x_axis_label)
    plt.ylabel(y_axis_label)
    plt.legend()
    plt.savefig(f"tests/benchmarks/results/{title}.png")
