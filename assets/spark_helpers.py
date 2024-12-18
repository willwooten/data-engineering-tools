# Standard Library Imports
from typing import Any, Optional, Dict

# Third-Party Library Imports
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

# Local Application/Project Imports
from assets.utilities import Result, StatusCode, Utilities


class SparkOptionsBuilder:
    """
    A helper class to construct Spark options dictionaries with a fluent interface.
    """

    def __init__(self):
        self.options = {}

    def _add(self, key: str, value: Any) -> "SparkOptionsBuilder":
        """
        Add an option to the options dictionary.

        :param key: The option key.
        :type key: str
        :param value: The option value.
        :type value: Any
        :return: The SparkOptionsBuilder instance for chaining.
        :rtype: SparkOptionsBuilder
        """
        self.options[key] = value
        return self

    def _build(self) -> Dict[str, Any]:
        """
        Build and return the options dictionary.

        :return: The constructed options dictionary.
        :rtype: Dict[str, Any]
        """
        return self.options

    @classmethod
    def csv_options(
        cls, path: str, header: bool = True, delimiter: str = ",", **kwargs
    ) -> Dict[str, Any]:
        """
        Create options for reading or writing CSV files.

        :param path: The file path for the CSV.
        :type path: str
        :param header: Whether the CSV has a header row. Defaults to True.
        :type header: bool
        :param delimiter: The delimiter used in the CSV. Defaults to ','.
        :type delimiter: str
        :param kwargs: Additional options to include.
        :return: A dictionary of CSV options.
        :rtype: Dict[str, Any]
        """
        builder = cls()
        builder._add("path", path)._add("header", str(header).lower())._add(
            "delimiter", delimiter
        )
        for key, value in kwargs.items():
            builder._add(key, value)
        return builder._build()

    @classmethod
    def json_options(
        cls, path: str, multiline: bool = False, **kwargs
    ) -> Dict[str, Any]:
        """
        Create options for reading or writing JSON files.

        :param path: The file path for the JSON.
        :type path: str
        :param multiline: Whether the JSON file is multiline. Defaults to False.
        :type multiline: bool
        :param kwargs: Additional options to include.
        :return: A dictionary of JSON options.
        :rtype: Dict[str, Any]
        """
        builder = cls()
        builder._add("path", path)._add("multiline", str(multiline).lower())
        for key, value in kwargs.items():
            builder._add(key, value)
        return builder._build()

    @classmethod
    def parquet_options(cls, path: str, **kwargs) -> Dict[str, Any]:
        """
        Create options for reading or writing Parquet files.

        :param path: The file path for the Parquet.
        :type path: str
        :param kwargs: Additional options to include.
        :return: A dictionary of Parquet options.
        :rtype: Dict[str, Any]
        """
        builder = cls()
        builder._add("path", path)
        for key, value in kwargs.items():
            builder._add(key, value)
        return builder._build()

    @classmethod
    def kafka_options(
        cls,
        bootstrap_servers: str,
        topic: str,
        starting_offsets: str = "latest",
        **kwargs,
    ) -> Dict[str, Any]:
        """
        Create options for reading from or writing to Kafka.

        :param bootstrap_servers: The Kafka bootstrap servers.
        :type bootstrap_servers: str
        :param topic: The Kafka topic to subscribe to or write to.
        :type topic: str
        :param starting_offsets: The starting offsets ('latest' or 'earliest'). Defaults to 'latest'.
        :type starting_offsets: str
        :param kwargs: Additional options to include.
        :return: A dictionary of Kafka options.
        :rtype: Dict[str, Any]
        """
        builder = cls()
        builder._add("kafka.bootstrap.servers", bootstrap_servers)._add(
            "subscribe", topic
        )._add("startingOffsets", starting_offsets)
        for key, value in kwargs.items():
            builder._add(key, value)
        return builder._build()


class SparkController:
    """
    A class to manage Apache Spark sessions and data transformations.
    """

    def __init__(
        self,
        utils: Utilities,
        app_name: str = "SparkApp",
        master: str = "local[*]",
        config: Optional[Dict[str, str]] = None,
    ):
        """
        Initialize the SparkController with configurations but delay SparkSession creation.

        :param utils: An instance of the Utilities class for logging and helper functions.
        :param app_name: The name of the Spark application.
        :type app_name: str
        :param master: The master URL for the cluster (e.g., 'local[*]', 'yarn', 'k8s').
        :type master: str
        :param config: Optional Spark configurations as a dictionary.
        :type config: Optional[Dict[str, str]]
        """
        self.utils = utils
        self.logger = self.utils.logger
        self.app_name = app_name
        self.master = master
        self.config = config
        self._spark: Optional[SparkSession] = None

    @property
    def spark(self) -> SparkSession:
        """
        Lazily initialize and return the SparkSession.

        :return: The SparkSession instance.
        :rtype: SparkSession
        """
        if self._spark is None:
            try:
                spark_builder = SparkSession.builder.master(self.master).appName(
                    self.app_name
                )
                if self.config:
                    for key, value in self.config.items():
                        spark_builder = spark_builder.config(key, value)

                self._spark = spark_builder.getOrCreate()
                self.logger.info(
                    f"Spark session initialized with app name '{self.app_name}' and master '{self.master}'."
                )
            except Exception as e:
                error_msg = f"Failed to initialize Spark session: {str(e)}"
                self.logger.error(error_msg)
                raise
        return self._spark

    def is_session_active(self) -> bool:
        """
        Check if the Spark session is active.

        :return: True if the session is active, False otherwise.
        """
        return self.spark is not None and not self.spark._jsc.sc().isStopped()

    def stop_session(self) -> None:
        """Stop the Spark session."""
        self.logger.info("Stopping Spark session.")
        self.spark.stop()

    def profile_data(self, df: DataFrame) -> Dict[str, Any]:
        """
        Generate a basic profile of the DataFrame.

        :param df: The DataFrame to profile.
        :type df: DataFrame
        :return: A dictionary with profiling information, including row count, columns, schema, and distinct counts.
        :rtype: Dict[str, Any]
        """
        profile = {
            "row_count": df.count(),
            "columns": df.columns,
            "schema": df.schema.simpleString(),
        }
        for col_name in df.columns:
            profile[col_name] = df.select(col_name).distinct().count()
        return profile

    def handle_missing_data(
        self, df: DataFrame, method: str = "drop", fill_value: Any = None
    ) -> DataFrame:
        """
        Handle missing data in the DataFrame.

        :param df: The DataFrame to process.
        :type df: DataFrame
        :param method: The method to handle missing data ('drop' to remove, 'fill' to replace).
        :type method: str
        :param fill_value: The value to fill missing data with if method is 'fill'. Defaults to None.
        :type fill_value: Any
        :return: The DataFrame with missing data handled according to the specified method.
        :rtype: DataFrame
        :raises ValueError: If an invalid method or fill_value is provided.
        """
        if method == "drop":
            return df.dropna()
        elif method == "fill" and fill_value is not None:
            return df.fillna(fill_value)
        else:
            raise ValueError("Invalid method or fill_value for handling missing data.")

    def read_from_sql(self, url: str, table: str, properties: Dict[str, str]) -> Result:
        """
        Read data from a SQL database.

        :param url: The JDBC URL for the database.
        :type url: str
        :param table: The table name to read from.
        :type table: str
        :param properties: JDBC connection properties, such as user and password.
        :type properties: Dict[str, str]
        :return: A Result object containing the loaded DataFrame or an error message.
        :rtype: Result
        """
        try:
            df = self.spark.read.jdbc(url=url, table=table, properties=properties)
            return Result(
                StatusCode.SUCCESS, "Data read successfully from SQL database.", df
            )
        except Exception as e:
            error_msg = f"Failed to read data from SQL database: {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def read_csv(self, path: str, **kwargs) -> Result:
        """
        Read data from a CSV source using SparkOptionsBuilder.

        :param path: The file path for the CSV.
        :type path: str
        :param kwargs: Additional options for reading the CSV.
        :return: A Result object containing the loaded DataFrame.
        :rtype: Result
        """
        options = SparkOptionsBuilder.csv_options(path, **kwargs)
        return self._read_data("csv", options)

    def read_json(self, path: str, **kwargs) -> Result:
        """
        Read data from a JSON source using SparkOptionsBuilder.

        :param path: The file path for the JSON.
        :type path: str
        :param kwargs: Additional options for reading the JSON.
        :return: A Result object containing the loaded DataFrame.
        :rtype: Result
        """
        options = SparkOptionsBuilder.json_options(path, **kwargs)
        return self._read_data("json", options)

    def read_parquet(self, path: str, **kwargs) -> Result:
        """
        Read data from a Parquet source using SparkOptionsBuilder.

        :param path: The file path for the Parquet.
        :type path: str
        :param kwargs: Additional options for reading the Parquet.
        :return: A Result object containing the loaded DataFrame.
        :rtype: Result
        """
        options = SparkOptionsBuilder.parquet_options(path, **kwargs)
        return self._read_data("parquet", options)

    def read_kafka(self, bootstrap_servers: str, topic: str, **kwargs) -> Result:
        """
        Read data from a Kafka source using SparkOptionsBuilder.

        :param bootstrap_servers: The Kafka bootstrap servers.
        :type bootstrap_servers: str
        :param topic: The Kafka topic to subscribe to.
        :type topic: str
        :param kwargs: Additional options for reading Kafka streams.
        :return: A Result object containing the loaded DataFrame.
        :rtype: Result
        """
        options = SparkOptionsBuilder.kafka_options(bootstrap_servers, topic, **kwargs)
        return self._read_data("kafka", options)

    def read_stream_from_kafka(
        self, bootstrap_servers: str, topic: str, **kwargs
    ) -> Result:
        """
        Read streaming data from a Kafka source.

        :param bootstrap_servers: The Kafka bootstrap servers (comma-separated list of broker addresses).
        :type bootstrap_servers: str
        :param topic: The Kafka topic to subscribe to.
        :type topic: str
        :param kwargs: Additional options for reading Kafka streams (e.g., startingOffsets, groupId).
        :type kwargs: Any
        :return: A Result object containing the streaming DataFrame or an error message.
        :rtype: Result
        """
        try:
            options = SparkOptionsBuilder.kafka_options(
                bootstrap_servers, topic, **kwargs
            )
            df = self.spark.readStream.format("kafka").options(**options).load()
            return Result(
                StatusCode.SUCCESS, "Streaming data read successfully from Kafka.", df
            )
        except Exception as e:
            error_msg = f"Failed to read streaming data from Kafka: {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def _read_data(
        self, source_type: str, options: Dict[str, Any], schema: Optional[Any] = None
    ) -> Result:
        """
        Internal method to read data from a specified source.

        :param source_type: The type of data source ('csv', 'json', 'parquet', 'kafka').
        :type source_type: str
        :param options: A dictionary of options for the data source.
        :type options: Dict[str, Any]
        :param schema: Optional schema definition for the DataFrame.
        :type schema: Optional[Any]
        :return: A Result object containing the loaded DataFrame.
        :rtype: Result
        """
        try:
            self.logger.info(
                f"Reading data from '{source_type}' with options: {options}"
            )
            reader = self.spark.read.options(**options)
            if schema:
                reader = reader.schema(schema)
            df = reader.format(source_type).load()
            return Result(
                StatusCode.SUCCESS, f"Data read successfully from '{source_type}'.", df
            )
        except Exception as e:
            error_msg = f"Failed to read data from '{source_type}': {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def write_to_sql(
        self, df: DataFrame, url: str, table: str, mode: str, properties: Dict[str, str]
    ) -> Result:
        """
        Write a DataFrame to a SQL database.

        :param df: The DataFrame to write to the database.
        :type df: DataFrame
        :param url: The JDBC URL for the database (e.g., 'jdbc:postgresql://host:port/dbname').
        :type url: str
        :param table: The table name where the DataFrame will be written.
        :type table: str
        :param mode: The write mode ('append', 'overwrite', 'ignore', 'error').
        :type mode: str
        :param properties: JDBC connection properties (e.g., {"user": "username", "password": "password"}).
        :type properties: Dict[str, str]
        :return: A Result object indicating success or failure with a message.
        :rtype: Result
        """
        try:
            df.write.jdbc(url=url, table=table, mode=mode, properties=properties)
            return Result(
                StatusCode.SUCCESS, f"Data written successfully to table '{table}'."
            )
        except Exception as e:
            error_msg = f"Failed to write data to SQL database: {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def write_csv(
        self, df: DataFrame, path: str, mode: str = "overwrite", **kwargs
    ) -> Result:
        """
        Write data to a CSV target using SparkOptionsBuilder.

        :param df: The DataFrame to write.
        :type df: DataFrame
        :param path: The file path for the CSV.
        :type path: str
        :param mode: The write mode ('append', 'overwrite'). Defaults to 'overwrite'.
        :type mode: str
        :param kwargs: Additional options for writing the CSV.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        options = SparkOptionsBuilder.csv_options(path, **kwargs)
        return self._write_data(df, "csv", options, mode)

    def write_json(
        self, df: DataFrame, path: str, mode: str = "overwrite", **kwargs
    ) -> Result:
        """
        Write data to a JSON target using SparkOptionsBuilder.

        :param df: The DataFrame to write.
        :type df: DataFrame
        :param path: The file path for the JSON.
        :type path: str
        :param mode: The write mode ('append', 'overwrite'). Defaults to 'overwrite'.
        :type mode: str
        :param kwargs: Additional options for writing the JSON.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        options = SparkOptionsBuilder.json_options(path, **kwargs)
        return self._write_data(df, "json", options, mode)

    def write_parquet(
        self, df: DataFrame, path: str, mode: str = "overwrite", **kwargs
    ) -> Result:
        """
        Write data to a Parquet target using SparkOptionsBuilder.

        :param df: The DataFrame to write.
        :type df: DataFrame
        :param path: The file path for the Parquet.
        :type path: str
        :param mode: The write mode ('append', 'overwrite'). Defaults to 'overwrite'.
        :type mode: str
        :param kwargs: Additional options for writing the Parquet.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        options = SparkOptionsBuilder.parquet_options(path, **kwargs)
        return self._write_data(df, "parquet", options, mode)

    def write_kafka(
        self,
        df: DataFrame,
        bootstrap_servers: str,
        topic: str,
        checkpoint_location: str,
        **kwargs,
    ) -> Result:
        """
        Write data to a Kafka target using SparkOptionsBuilder.

        :param df: The DataFrame to write.
        :type df: DataFrame
        :param bootstrap_servers: The Kafka bootstrap servers.
        :type bootstrap_servers: str
        :param topic: The Kafka topic to write to.
        :type topic: str
        :param checkpoint_location: The location for checkpointing.
        :type checkpoint_location: str
        :param kwargs: Additional options for writing to Kafka.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        options = SparkOptionsBuilder.kafka_options(bootstrap_servers, topic, **kwargs)
        try:
            self.logger.info(
                f"Writing data to Kafka topic '{topic}' with options: {options}"
            )

            if "key" not in df.columns or "value" not in df.columns:
                error_msg = "DataFrame must contain 'key' and 'value' columns to write to Kafka."
                self.logger.error(error_msg)
                return Result(StatusCode.FAILURE, error_msg)

            query = (
                df.writeStream.format("kafka")
                .options(**options)
                .option("checkpointLocation", checkpoint_location)
                .start()
            )

            query.awaitTermination()
            return Result(
                StatusCode.SUCCESS,
                f"Data written successfully to Kafka topic '{topic}'.",
            )
        except Exception as e:
            error_msg = f"Failed to write data to Kafka topic '{topic}': {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def _write_data(
        self, df: DataFrame, target_type: str, options: Dict[str, Any], mode: str
    ) -> Result:
        """
        Internal method to write a DataFrame to a specified target.

        :param df: The DataFrame to write.
        :type df: DataFrame
        :param target_type: The type of target ('csv', 'json', 'parquet').
        :type target_type: str
        :param options: A dictionary of options for writing the data.
        :type options: Dict[str, Any]
        :param mode: The write mode ('append', 'overwrite').
        :type mode: str
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            self.logger.info(
                f"Writing data to '{target_type}' with options: {options} and mode: '{mode}'"
            )
            df.write.format(target_type).options(**options).mode(mode).save()
            return Result(
                StatusCode.SUCCESS, f"Data written successfully to '{target_type}'."
            )
        except Exception as e:
            error_msg = f"Failed to write data to '{target_type}': {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)
