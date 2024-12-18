# Data Engineering Tools

This repository provides a suite of tools designed to facilitate data engineering tasks, including job processing, data handling, and interaction with various data protocols like Ethereum. The tools leverage modern Python features such as asynchronous programming, multiprocessing, and Apache Spark for efficient and scalable data processing.

## How to Run

Use the following command to execute the `main.py` script:

```bash
python main.py \
    -j dummy-job-name \
    -n 2024.12.15.10.02.55.542 \
    -b "development" \
    -v ETHEREUM \
    -tcf target_calls_1000.json \
    -u "target=blocks|start_block=125|end_block=694"
```

### Command-Line Arguments

| Argument                       | Description                                                         |
|--------------------------------|---------------------------------------------------------------------|
| **`-j` / `--job_name`**        | Specifies the job name.                                             |
| **`-n` / `--job_run_id`**      | Unique job run identifier (e.g., timestamp).                        |
| **`-b` / `--bucket`**          | S3 bucket name or environment (e.g., `"development"`).              |
| **`-v` / `--protocol_cd`**     | Protocol code to run (e.g., `ETHEREUM`).                            |
| **`-tcf` / `--target_calls_filename`** | Path to the JSON file containing target calls.                |
| **`-u` / `--user_input`**      | User input specifying details like target and block range.          |

### User Input Format

The `-u` parameter should be formatted as:

```text
"target=blocks|start_block=125|end_block=694"
```

## Tools and Features

### 1. **Asynchronous Programming**

The repository uses asynchronous functions to manage I/O-bound operations efficiently. This is particularly useful when dealing with API calls, database queries, and file operations, allowing the application to handle multiple tasks concurrently without blocking execution.

**Why it is useful:**
- Improves performance when handling high-latency operations.
- Keeps the application responsive and efficient.

### 2. **Multiprocessing**

The repository supports multiprocessing to parallelize CPU-bound tasks. This is achieved through Python's `multiprocessing` module, which allows tasks to run concurrently across multiple CPU cores.

**Why it is useful:**
- Speeds up processing of large datasets by utilizing multiple CPU cores.
- Reduces the execution time of compute-heavy tasks.

### 3. **Apache Spark Integration**

The tools include support for Apache Spark through the `SparkController` class. Spark enables distributed data processing, making it suitable for handling large datasets that do not fit into memory.

**Why it is useful:**
- Provides scalable and distributed data processing capabilities.
- Offers efficient handling of large-scale data transformations and analytics.

### 4. **Kafka Integration**

The repository includes Kafka tools for managing real-time data streams. It supports creating Kafka topics, producing messages, and consuming messages for real-time data pipelines.

**Why it is useful:**
- Facilitates real-time data ingestion and processing.
- Enables building robust streaming applications.

### 5. **Utilities for File Operations and Logging**

The `Utilities` class provides helper functions for logging, file operations, and job management. It supports reading and writing files in various formats like JSON, CSV, and Parquet.

**Why it is useful:**
- Simplifies handling of different file formats.
- Provides consistent logging and error handling throughout the application.

### 6. **Retry Mechanism**

A robust retry mechanism is included to handle transient errors gracefully. The `retry` decorator allows functions to automatically retry on failure with customizable parameters like total attempts and backoff strategy.

**Why it is useful:**
- Increases the reliability of network and API interactions.
- Reduces manual intervention when handling intermittent issues.

## Logging and Results

Logs and results will be saved to the designated directories as specified in the configurations.

## Additional Information

For more details on the available protocols and configurations, refer to the code and comments in `main.py` and the `assets` directory.
