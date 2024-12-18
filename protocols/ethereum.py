# Standard Library Imports
import datetime
import json
import multiprocessing
import traceback
from typing import Any, Dict, Literal

# Local Application/Project Imports
from assets.pool_control import PoolProcesses
from assets.request_helpers import Requests
from assets.s3_helpers import S3Helpers
from assets.utilities import Result, StatusCode, Utilities


class EthereumClass(object):
    """
    A class to handle Ethereum-specific operations.
    """

    def __init__(self, utils: Utilities, localstack: bool = False):
        """
        Initialize the EthereumClass with utility functions, logger, and necessary configurations.

        :param utils: An instance of the Utilities class for logging, configuration, and helper functions.
        :type utils: Utilities
        """
        self.utils = utils
        self.logger = self.utils.logger

        self.pool = PoolProcesses(self.utils)
        self.requests = Requests(self.utils)
        self.s3 = S3Helpers(self.utils, localstack=localstack)

        self.target = self.utils.args.user_input.target
        self.start_block = self.utils.args.user_input.start_block
        self.end_block = self.utils.args.user_input.end_block

        self.target_x_auth_token = "auth-token"

        self.headers = {
            "Content-Type": "application/json",
            "X-Auth-Token": self.target_x_auth_token,
        }

        self.file_time = datetime.datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    def start_ethereum(self, **kwargs: Dict[str, Any]) -> Result:
        """
        Start the Ethereum process based on the specified target.

        :param kwargs: Additional arguments passed to the target function.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            # This should map to the target-XXX provided by the user_input
            target_function_map = {
                "blocks": self.start_blocks,
                # "logs": self.start_logs,
            }

            if self.target in target_function_map:
                result = target_function_map[self.target](**kwargs)
            else:
                raise Exception(
                    "You must provide a valid target in user inputs (ex. target=blocks)"
                )

        except Exception as e:
            error_msg = f"Error starting {self.target}: {e}"
            self.logger.error(str(traceback.format_exc()))
            result = Result(StatusCode.FAILURE, error_msg)

        return result

    def process_call(
        self,
        key: str,
        method: str,
        target_url: str,
        file_prefix: str,
        status_dict: Dict,
        batch_no: int,
        call_id: int,
        payload: Dict,
        file_type: Literal[
            "txt", "json", "csv", "parquet"
        ] = "json",  # Restrict file_type to specified choices
    ) -> None:
        """
        Generalized function to retrieve data from a specified endpoint and upload it to S3 in various file formats.

        :param key: The unique key for tracking the status of the process.
        :type key: str
        :param method: The method name for the request (e.g., 'blocks', 'logs').
        :type method: str
        :param target_url: The URL of the endpoint to call.
        :type target_url: str
        :param file_prefix: The prefix for the output file name.
        :type file_prefix: str
        :param status_dict: A dictionary to update the status of the process.
        :type status_dict: Dict
        :param batch_no: The batch number being processed.
        :type batch_no: int
        :param call_id: The ID of the current call.
        :type call_id: int
        :param payload: The JSON payload to send in the request.
        :type payload: Dict
        :param file_type: The type of file to create ('json', 'csv', 'parquet').
        :type file_type: Literal["json", "csv", "parquet"]
        """
        self.pool.assign_status(
            {
                "STATUS": "RUNNING",
                "DETAILS": f"{method} ::: {target_url} -> {file_prefix}",
                "ENDED_AT": None,
            },
            status_dict,
            key,
        )

        try:
            self.logger.info(f"batch -> {batch_no} ::: call_id -> {call_id}")

            response_data = self.requests.post(
                target_url,
                payload=json.dumps(payload),
                headers=self.headers,
            )

            file_name = f"{file_prefix}_{method}_batch_{batch_no}_{call_id}_{self.file_time}.{file_type}"
            local_file_path = f"{self.utils.local_data_path}/{file_name}"

            # self.utils.write_local_file(local_file_path, response_data["result"])

            # self.s3.upload_s3(
            #     f"{self.utils.args.protocol_cd}/{method}/{file_name}",
            #     local_file_path,
            #     content_type=True,
            # )

            self.pool.assign_status(
                {
                    "STATUS": "COMPLETED",
                    "DETAILS": f"{file_name} uploaded to S3",
                    "ENDED_AT": datetime.datetime.now(),
                },
                status_dict,
                key,
            )

        except Exception as e:
            self.pool.assign_status(
                {
                    "STATUS": "FAILED",
                    "DETAILS": f"{str(e)}",
                    "ENDED_AT": datetime.datetime.now(),
                },
                status_dict,
                key,
            )

            self.logger.error(str(traceback.format_exc()))

    def get_block(
        self,
        key: str,
        call_id: int,
        batch_no: int,
        block_number: int,
        status_dict: Dict,
        target_url: str,
    ) -> None:
        """
        Retrieve Ethereum block data and send it to S3 storage using the `process_call` function.

        The function constructs the payload for the `eth_getBlockByNumber` RPC call and passes it
        to `process_call` for execution and subsequent upload to S3 storage.

        :param key: The unique key for tracking the status of the process.
        :type key: str
        :param call_id: The ID of the current call.
        :type call_id: int
        :param batch_no: The batch number being processed.
        :type batch_no: int
        :param block_number: The block number to retrieve.
        :type block_number: int
        :param status_dict: A dictionary to update the status of the process.
        :type status_dict: Dict
        :param target_url: The URL of the Ethereum endpoint.
        :type target_url: str
        """
        method = "eth_getBlockByNumber"
        payload = {
            "id": 1,
            "jsonrpc": "2.0",
            "method": method,
            "params": [hex(block_number), True],
        }

        self.process_call(
            key=key,
            method=method,
            target_url=target_url,
            file_prefix=f"ethereum_block_{block_number}",
            status_dict=status_dict,
            batch_no=batch_no,
            call_id=call_id,
            payload=payload,
            file_type="json",
        )

    def start_blocks(self, **kwargs: Dict[str, Any]) -> Result:
        """
        Start processing Ethereum blocks within the specified range.

        :param kwargs: Additional arguments containing the list of target calls.
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            # Retrieve the target calls passed via kwargs
            target_calls = kwargs.get("target_calls")
            if not target_calls:
                raise ValueError("target_calls must be provided as a keyword argument.")

            # Process each batch of blocks
            self.pool.run_pool_processes(self.get_block, target_calls)

            return Result(StatusCode.SUCCESS, "Blocks processed successfully.")

        except Exception as e:
            error_msg = f"Error at start_blocks: {e}"
            self.utils.logger.error(str(traceback.format_exc()))
            return Result(StatusCode.FAILURE, error_msg)


if __name__ == "__main__":
    multiprocessing.freeze_support()
