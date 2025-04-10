import logging
import tempfile
import time

from dumbdb.append_only_dbms_with_hash_indexes import \
    AppendOnlyDBMSWithHashIndexes
from tests.benchmarks.common import (DATASET_SIZES, NUM_ITERATIONS,
                                     generate_user_data, populate_database,
                                     setup_database, plot_results)


def test_startup_benchmark_with_hash_indexes():
    """Test the startup performance of AppendOnlyDBMSWithHashIndexes"""
    logging.info("\n=== TESTING DBMS WITH HASH INDEXES ===")

    # Arrays to store timing results
    total_times = []
    avg_times = []

    for size in DATASET_SIZES:
        logging.info(f"\n--- Startup benchmark with {size} records ---")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup and populate database
            db_dir, dbms = setup_database(temp_dir, with_indexes=True)
            populate_database(dbms, size)

            # Release the database to ensure clean startup measurements
            dbms = None

            # Measure startup time over multiple iterations
            total_startup_time = 0
            for i in range(NUM_ITERATIONS):
                start_time = time.time()
                dbms = AppendOnlyDBMSWithHashIndexes(root_dir=db_dir)
                dbms.use_database("test_db")
                total_startup_time += (time.time() - start_time)

            avg_startup_time = total_startup_time / NUM_ITERATIONS

            # Store results in arrays
            total_times.append(total_startup_time)
            avg_times.append(avg_startup_time)

            # Log both total and average times
            logging.info(
                f"DBMS with hash indexes total startup time ({NUM_ITERATIONS} iterations): {total_startup_time*1000:.4f} ms")
            logging.info(
                f"DBMS with hash indexes average startup time per iteration: {avg_startup_time*1000:.4f} ms")

    # Log final array of times
    logging.info("\n=== SUMMARY OF DBMS WITH HASH INDEXES STARTUP TIMES ===")
    logging.info(f"Total startup times (ms): {[t*1000 for t in total_times]}")
    logging.info(f"Average startup times (ms): {[t*1000 for t in avg_times]}")

    plot_results(
        title="DBMS_With_Hash_Indexes_Startup_Time",
        x_axis_data=DATASET_SIZES,
        y_axis=[
            {
                "label": f"Average Startup Time (averaged over {NUM_ITERATIONS} trials)",
                "values": [t*1000 for t in avg_times]
            }
        ],
        x_axis_label="Dataset Size",
        y_axis_label="Time (ms)"
    )


def test_insert_benchmark_with_hash_indexes():
    """Test the insert performance of AppendOnlyDBMSWithHashIndexes"""
    logging.info("\n=== TESTING DBMS WITH HASH INDEXES ===")

    # Arrays to store timing results
    total_times = []
    avg_times = []

    for size in DATASET_SIZES:
        logging.info(
            f"\n--- Insert benchmark with {size} existing records ---")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup and populate database
            db_dir, dbms = setup_database(temp_dir, with_indexes=True)
            populate_database(dbms, size)

            # Measure insert time over multiple iterations
            total_insert_time = 0
            for i in range(NUM_ITERATIONS):
                # Create 100 new users for each iteration
                start_time = time.time()
                user = generate_user_data(size + i*100)
                dbms.insert("users", user)
                total_insert_time += (time.time() - start_time)

            avg_insert_time = total_insert_time / NUM_ITERATIONS

            # Store results in arrays
            total_times.append(total_insert_time)
            avg_times.append(avg_insert_time)

            # Log both total and average times
            logging.info(
                f"DBMS with hash indexes total time to insert 100 records ({NUM_ITERATIONS} iterations): {total_insert_time*1000:.4f} ms")
            logging.info(
                f"DBMS with hash indexes average time to insert 100 records (per iteration): {avg_insert_time*1000:.4f} ms")

    # Log final arrays of times
    logging.info("\n=== SUMMARY OF DBMS WITH HASH INDEXES INSERT TIMES ===")
    logging.info(
        f"Total insert times for 100 records: {[t*1000 for t in total_times]}")
    logging.info(
        f"Average insert times per iteration of 100 records: {[t*1000 for t in avg_times]}")

    plot_results(
        title="DBMS_With_Hash_Indexes_Insert_Time",
        x_axis_data=DATASET_SIZES,
        y_axis=[
            {
                "label": f"Average Insert Time (averaged over {NUM_ITERATIONS} trials)",
                "values": [t*1000 for t in avg_times]
            }
        ],
        x_axis_label="Dataset Size",
        y_axis_label="Time (ms)"
    )


def test_query_benchmark_with_hash_indexes():
    """Test the query performance of AppendOnlyDBMSWithHashIndexes"""
    logging.info("\n=== TESTING DBMS WITH HASH INDEXES ===")

    # Arrays to store timing results
    total_times = []
    avg_times = []

    for size in DATASET_SIZES:
        logging.info(f"\n--- Query benchmark with {size} records ---")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup and populate database
            db_dir, dbms = setup_database(temp_dir, with_indexes=True)
            populate_database(dbms, size)

            # Measure query time over multiple iterations
            total_query_time = 0
            for i in range(NUM_ITERATIONS):
                start_time = time.time()
                dbms.query("users", {"id": str((i*100) % size)})
                total_query_time += (time.time() - start_time)

            avg_query_time = total_query_time / NUM_ITERATIONS

            # Store results in arrays
            total_times.append(total_query_time)
            avg_times.append(avg_query_time)

            # Log both total and average times
            logging.info(
                f"DBMS with hash indexes total time to query 1 record ({NUM_ITERATIONS} iterations): {total_query_time*1000:.4f} ms")
            logging.info(
                f"DBMS with hash indexes average time to query 1 record (per iteration): {avg_query_time*1000:.4f} ms")

    # Log final arrays of times
    logging.info("\n=== SUMMARY OF DBMS WITH HASH INDEXES QUERY TIMES ===")
    logging.info(
        f"Total query times for 1 record x {NUM_ITERATIONS} iterations (ms): {[t*1000 for t in total_times]}")
    logging.info(
        f"Average query times per iteration of 1 record (ms): {[t*1000 for t in avg_times]}")

    plot_results(
        title="DBMS_With_Hash_Indexes_Query_Time",
        x_axis_data=DATASET_SIZES,
        y_axis=[
            {
                "label": f"Average Query Time (averaged over {NUM_ITERATIONS} trials)",
                "values": [t*1000 for t in avg_times]
            }
        ],
        x_axis_label="Dataset Size",
        y_axis_label="Time (ms)"
    )


def test_mixed_workload_benchmark_with_hash_indexes():
    """Test overall performance with realistic workload mix using AppendOnlyDBMSWithHashIndexes"""
    logging.info("\n=== TESTING DBMS WITH HASH INDEXES ===")

    # Arrays to store timing results
    total_times = []
    avg_times = []

    for size in DATASET_SIZES:
        logging.info(f"\n--- Mixed workload benchmark with {size} records ---")

        with tempfile.TemporaryDirectory() as temp_dir:
            # Setup and populate database
            db_dir, dbms = setup_database(temp_dir, with_indexes=True)
            populate_database(dbms, size)

            # Perform mixed workload (70% reads, 20% inserts, 10% updates)
            logging.info(
                "\nPerforming mixed workload (70% reads, 20% inserts, 10% updates):")

            total_mixed_time = 0
            for i in range(NUM_ITERATIONS):
                start_time = time.time()
                for j in range(100):  # 100 operations per iteration
                    operation_type = j % 10  # Determines operation type
                    if operation_type < 7:  # 70% reads
                        dbms.query("users", {"id": str((i*100 + j) % size)})
                    elif operation_type < 9:  # 20% inserts
                        user = generate_user_data(size + i*100 + j)
                        dbms.insert("users", user)
                    else:  # 10% updates
                        user_id = str((i*100 + j) % size)
                        user = generate_user_data(int(user_id))
                        # Increment age
                        user["age"] = str(int(user["age"]) + 1)
                        dbms.update("users", user)
                total_mixed_time += (time.time() - start_time)

            avg_mixed_time = total_mixed_time / NUM_ITERATIONS

            # Store results in arrays
            total_times.append(total_mixed_time)
            avg_times.append(avg_mixed_time)

            # Log both total and average times
            logging.info(
                f"DBMS with hash indexes total time for mixed workload of 100 operations x {NUM_ITERATIONS} iterations: {total_mixed_time*1000:.4f} ms")
            logging.info(
                f"DBMS with hash indexes average time for mixed workload of 100 operations (per iteration): {avg_mixed_time*1000:.4f} ms")

    # Log final arrays of times
    logging.info(
        "\n=== SUMMARY OF DBMS WITH HASH INDEXES MIXED WORKLOAD TIMES ===")
    logging.info(
        f"Total mixed workload times for 100 operations x {NUM_ITERATIONS} iterations (ms): {[t*1000 for t in total_times]}")
    logging.info(
        f"Average mixed workload times per iteration of 100 operations (ms): {[t*1000 for t in avg_times]}")

    plot_results(
        title="DBMS_With_Hash_Indexes_Mixed_Workload_Time",
        x_axis_data=DATASET_SIZES,
        y_axis=[
            {
                "label": f"Average Mixed Workload Time (averaged over {NUM_ITERATIONS} trials)",
                "values": [t*1000 for t in avg_times]
            }
        ],
        x_axis_label="Dataset Size",
        y_axis_label="Time (ms)"
    )
