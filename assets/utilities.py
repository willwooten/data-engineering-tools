# Standard Library Imports
import csv
import json
import mimetypes
import os
import pathlib
import shutil
import traceback
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Literal, Optional, Union

# Third-Party Library Imports
import pandas as pd

# Local Application/Project Imports
from assets.arguments import ArgumentHandler
from assets.exceptions import (
    CheckRaiseException,
    LocalFileException,
    TargetCallsException,
)
from assets.log_helpers import ApplicationLogging


class StatusCode(Enum):
    SUCCESS = 0
    FAILURE = 1


@dataclass
class Result:
    ret_cd: StatusCode
    ret_msg: str
    data: Optional[Any] = field(default=None)


class Utilities:
    """
    A class providing utility functions for logging, file operations, and job information management.

    :param args: A list of command-line arguments to initialize the utility.
    :type args: List[str]
    """

    def __init__(self, args: List[str]):
        """
        Initialize the Utilities class with arguments and a logger.

        :param args: A list of command-line arguments.
        :type args: List[str]
        """

        self.args = ArgumentHandler(args).load_arguments()

        # initialize logging
        self.log_helper = ApplicationLogging(self.args)
        self.logger = self.log_helper.logger

        self.local_data_path = os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", "data/temp")
        )
        os.makedirs(self.local_data_path, exist_ok=True)

    @staticmethod
    def check_raise(result: Result) -> None:
        """
        Raise an exception if the result indicates failure.

        :param result: The result object to check.
        :type result: Result
        :raises Exception: If the result status code is FAILURE.
        """
        if result.ret_cd == StatusCode.FAILURE:
            raise CheckRaiseException(f"\U0001F61F {result.ret_msg}")

    def write_local_file(
        self,
        local_file: str,
        content: Union[
            str, bytes, List[str], Dict, List[Dict], List[List], pd.DataFrame
        ],
        file_type: Literal["txt", "json", "csv", "parquet"] = "json",
    ) -> None:
        """
        Write content to a local file in TXT, JSON, CSV, or Parquet format.

        :param local_file: The path to the local file to write.
        :type local_file: str
        :param content: The content to write to the file.
        :type content: Union[str, bytes, List[str], Dict, List[Dict], List[List], pd.DataFrame]
        :param file_type: The type of file to write ('txt', 'json', 'csv', 'parquet').
        :type file_type: Literal["txt", "json", "csv", "parquet"]
        :raises ValueError: If the file type is unsupported or content is invalid for the specified file type.
        :raises LocalFileException: If writing to the file fails.
        """
        try:
            if file_type == "json":
                self._write_json(local_file, content)
            elif file_type == "txt":
                self._write_txt(local_file, content)
            elif file_type == "csv":
                self._write_csv(local_file, content)
            elif file_type == "parquet":
                self._write_parquet(local_file, content)
            else:
                raise ValueError(f"Unsupported file type: {file_type}")

            self.logger.info(f"Successfully wrote local file {local_file}")
            result = Result(StatusCode.SUCCESS, "Success")

        except LocalFileException as e:
            result = Result(
                StatusCode.FAILURE,
                f"**** Error on -> write_local_file. Message: {str(e)}. Filename: {local_file}",
            )
            self.logger.error(result.ret_msg)
            self.logger.error(str(traceback.format_exc()))

        self.check_raise(result)

    def _write_json(self, local_file: str, content: Union[Dict, List]) -> None:
        """
        Write content to a JSON file.

        :param local_file: The path to the local file to write.
        :type local_file: str
        :param content: The content to write to the file, must be a dict or a list.
        :type content: Union[Dict, List]
        :raises ValueError: If the content is not a dict or a list.
        """
        if not isinstance(content, (dict, list)):
            raise ValueError("Content must be a dictionary or a list for JSON files.")
        with open(local_file, "w", encoding="utf-8") as out:
            json.dump(content, out, indent=2, default=str)

    def _write_txt(
        self, local_file: str, content: Union[str, bytes, List[str]]
    ) -> None:
        """
        Write content to a TXT file.

        :param local_file: The path to the local file to write.
        :type local_file: str
        :param content: The content to write, must be str, bytes, or a list of strings.
        :type content: Union[str, bytes, List[str]]
        :raises ValueError: If the content is not a valid type for TXT files.
        """
        if isinstance(content, (str, bytes)):
            mode = "w" if isinstance(content, str) else "wb"
            with open(local_file, mode, encoding="utf-8") as out:
                out.write(content)
        elif isinstance(content, list) and all(
            isinstance(line, str) for line in content
        ):
            with open(local_file, "w", encoding="utf-8") as out:
                out.write("\n".join(content))
        else:
            raise ValueError(
                "Content must be a string, bytes, or a list of strings for TXT files."
            )

    def _write_csv(
        self, local_file: str, content: Union[pd.DataFrame, List[Dict], List[List]]
    ) -> None:
        """
        Write content to a CSV file.

        :param local_file: The path to the local file to write.
        :type local_file: str
        :param content: The content to write, must be a DataFrame, list of dicts, or list of lists.
        :type content: Union[pd.DataFrame, List[Dict], List[List]]
        :raises ValueError: If the content is not a valid type for CSV files.
        """
        if isinstance(content, pd.DataFrame):
            content.to_csv(local_file, index=False)
        elif isinstance(content, list) and all(
            isinstance(row, dict) for row in content
        ):
            pd.DataFrame(content).to_csv(local_file, index=False)
        elif isinstance(content, list) and all(
            isinstance(row, list) for row in content
        ):
            with open(local_file, mode="w", newline="", encoding="utf-8") as out:
                writer = csv.writer(out)
                writer.writerows(content)
        else:
            raise ValueError(
                "Content must be a DataFrame, a list of dictionaries, or a list of lists for CSV files."
            )

    def _write_parquet(
        self, local_file: str, content: Union[pd.DataFrame, List[Dict]]
    ) -> None:
        """
        Write content to a Parquet file.

        :param local_file: The path to the local file to write.
        :type local_file: str
        :param content: The content to write, must be a DataFrame or a list of dicts.
        :type content: Union[pd.DataFrame, List[Dict]]
        :raises ValueError: If the content is not a valid type for Parquet files.
        """
        if isinstance(content, pd.DataFrame):
            content.to_parquet(local_file, index=False)
        elif isinstance(content, list) and all(
            isinstance(row, dict) for row in content
        ):
            pd.DataFrame(content).to_parquet(local_file, index=False)
        else:
            raise ValueError(
                "Content must be a DataFrame or a list of dictionaries for Parquet files."
            )

    def read_local_file(self, local_file: str) -> Result:
        """
        Read content from a local file in TXT, JSON, CSV, or Parquet format.

        The function supports the following file extensions:
        - **.txt**: Reads the file as a plain text string.
        - **.json**: Parses the file content into a dictionary or list.
        - **.csv**: Reads the file into a pandas DataFrame.
        - **.parquet**: Reads the file into a pandas DataFrame.

        The function performs a `check_raise` to raise an exception if the operation fails,
        and returns a `Result` object containing the file data on success or error details on failure.

        :param local_file: The path to the local file to read, relative to the 'data' directory.
        :type local_file: str
        :return: A Result object containing the read content or an error message.
        :rtype: Result
        :raises LocalFileException: If the file cannot be read or the file type is unsupported.
        """
        try:
            data_path = f"data/{local_file}"
            self.logger.info(f"Reading local file {data_path}...")

            file_extension = pathlib.Path(local_file).suffix.lower()

            if file_extension == ".json":
                with open(data_path, "r", encoding="utf-8") as f:
                    ret_data = json.load(f)

            elif file_extension == ".txt":
                with open(data_path, "r", encoding="utf-8") as f:
                    ret_data = f.read()

            elif file_extension == ".csv":
                ret_data = pd.read_csv(data_path)

            elif file_extension == ".parquet":
                ret_data = pd.read_parquet(data_path)

            else:
                raise ValueError(
                    f"Unsupported file type: {file_extension}. Supported types are: .txt, .json, .csv, .parquet"
                )

            result = Result(StatusCode.SUCCESS, "Success", ret_data)

        except LocalFileException as e:
            result = Result(
                StatusCode.FAILURE,
                f"**** Error on -> read_local_file. Message: {str(e)}. Filename: {local_file}",
                None,
            )
            self.logger.error(result.ret_msg)
            self.logger.error(str(traceback.format_exc()))

        self.check_raise(result)
        return result

    def load_target_calls(self) -> List[Dict]:
        """
        Load target calls from a local file specified in the 'target_calls_filename' argument.

        The file content must be a list of dictionaries. If the format is incorrect,
        a ValueError is raised.

        :return: The loaded targets as a list of dictionaries.
        :rtype: List[Dict]
        :raises Exception: If the file cannot be read or the content format is invalid.
        """
        if self.args.target_calls_filename:
            result: Result = self.read_local_file(self.args.target_calls_filename)

            self.check_raise(result)

            if isinstance(result.data, list) and all(
                isinstance(item, dict) for item in result.data
            ):
                self.logger.info(
                    f"Successfully read and validated data from {self.args.target_calls_filename}"
                )
                return result.data
            else:
                raise ValueError(
                    f"The content of {self.args.target_calls_filename} must be a list of dictionaries."
                )

        raise TargetCallsException("Failed to load targetted calls.")

    @staticmethod
    def get_content_type(file_name: str) -> str:
        """
        Determine the content type based on the file extension.

        :param file_name: The name of the file.
        :return: The content type of the file.
        """
        content_type, _ = mimetypes.guess_type(file_name)
        return content_type

    def remove_directory(self, folder_path: str) -> None:
        """
        Remove the specified directory and its contents.

        This method checks if the provided path exists and is a directory before
        deleting it and all of its contents.

        :param folder_path: The path to the directory to be removed.
        :type folder_path: str
        :raises FileNotFoundError: If the specified path does not exist.
        :raises NotADirectoryError: If the specified path is not a directory.
        """
        if os.path.exists(folder_path) and os.path.isdir(folder_path):
            shutil.rmtree(folder_path)
        elif not os.path.exists(folder_path):
            raise FileNotFoundError(f"The path '{folder_path}' does not exist.")
        else:
            raise NotADirectoryError(f"The path '{folder_path}' is not a directory.")

    @staticmethod
    def unnest_lists(nested_lists: List[List[Dict]]) -> List[Dict]:
        """
        Flatten a list of lists into a single list.

        This function takes a list of lists, where each sublist contains dictionaries,
        and flattens it into a single list containing all the dictionaries.

        :param nested_lists: A list of lists, where each sublist contains dictionaries.
        :type nested_lists: List[List[Dict]]
        :return: A single list containing all dictionaries from the nested lists.
        :rtype: List[Dict]
        """
        return [item for sublist in nested_lists for item in sublist]
