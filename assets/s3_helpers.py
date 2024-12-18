# Standard Library Imports
import os
import traceback
import subprocess
import time
from typing import Optional

# Third-Party Library Imports
import boto3

# Local Application/Project Imports
from assets import retry_dec
from assets.utilities import Result, StatusCode, Utilities


class LocalStackManager:
    """
    Manages the lifecycle of LocalStack, including starting and stopping it.

    This class uses the provided Utilities instance for logging purposes.
    """

    def __init__(self, utils: Utilities):
        """
        Initializes the LocalStackManager with a Utilities instance.

        :param utils: An instance of the Utilities class for logging and utility methods.
        :type utils: Utilities
        """
        self.logger = utils.logger

    def start_localstack(self) -> None:
        """
        Starts LocalStack and waits for it to become available.

        This method logs the start process and waits for 10 seconds
        to give LocalStack time to initialize.
        """
        self.logger.info("Starting LocalStack...")
        self._start_localstack()
        time.sleep(5)  # Give LocalStack some time to start

    def _start_localstack(self) -> None:
        """
        Runs the LocalStack start command.

        Logs an error if the LocalStack process fails to start.
        """
        try:
            subprocess.run(["localstack", "start", "-d"], timeout=240, check=True)
        except subprocess.CalledProcessError as error:
            self.logger.error(f"Error starting LocalStack: {error}")

    def stop_localstack(self) -> None:
        """
        Stops LocalStack.

        Logs the stop process and captures any errors if the stop command fails.
        """
        self.logger.info("Stopping LocalStack...")
        try:
            subprocess.run(["localstack", "stop"], check=True)
        except subprocess.CalledProcessError as error:
            self.logger.error(f"Error stopping LocalStack: {error}")


class S3Helpers:
    """
    A helper class for uploading files to an S3 bucket with retry capabilities.

    :param utils: An instance of the Utilities class for logging and helper functions.
    :type utils: Utilities
    :param endpoint_url: The endpoint URL for S3 (for LocalStack or custom endpoints).
    :type endpoint_url: Optional[str]
    :param region_name: The region name for the S3 client.
    :type region_name: str
    :param aws_access_key_id: AWS access key ID for authentication.
    :type aws_access_key_id: Optional[str]
    :param aws_secret_access_key: AWS secret access key for authentication.
    :type aws_secret_access_key: Optional[str]
    """

    def __init__(
        self,
        utils: Utilities,
        localstack: bool = False,
    ):
        self.utils = utils
        self.logger = self.utils.logger

        self.s3_bucket = self.utils.args.s3_config.s3_bucket
        self.region_name = self.utils.args.s3_config.region_name
        self.aws_access_key_id = self.utils.args.s3_config.aws_access_key_id
        self.aws_secret_access_key = self.utils.args.s3_config.aws_secret_access_key

        if localstack:
            self.s3_endpoint = "http://localhost:4566"
            LocalStackManager(self.utils).start_localstack()
            self.create_bucket(self.s3_bucket)
        else:
            self.s3_endpoint = None

    def _get_s3_client(self):
        """Create and return a new S3 client."""
        return boto3.client(
            "s3",
            endpoint_url=self.s3_endpoint,
            region_name=self.region_name,
            aws_access_key_id=self.aws_access_key_id,
            aws_secret_access_key=self.aws_secret_access_key,
        )

    @retry_dec.retry(Exception, total_tries=3, initial_wait_secs=10)
    def s3_upload_retry(
        self,
        file_key: str,
        local_file: str,
        bucket: Optional[str] = None,
        content_type: bool = False,
        logger=None,
    ):
        """
        Upload a file to an S3 bucket with retry logic.

        :param file_key: The key (path) where the file will be stored in the S3 bucket.
        :type file_key: str
        :param local_file: The path to the local file to be uploaded.
        :type local_file: str
        :param bucket: The name of the S3 bucket. Defaults to the instance's `s3_bucket` attribute.
        :type bucket: Optional[str]
        :param content_type: Whether to set the content type of the file based on its extension.
        :type content_type: bool
        :param logger: The logger instance used by the retry decorator. Defaults to None.
        :type logger: Optional[logging.Logger]
        :raises Exception: If the upload fails after the maximum number of retries.
        """
        bucket = bucket or self.s3_bucket
        extra_args = {}

        if content_type:
            content_type_value = self.utils.get_content_type(local_file)
            extra_args["ContentType"] = content_type_value

        s3_client = self._get_s3_client()
        try:
            s3_client.upload_file(local_file, Bucket=bucket, Key=file_key)
        finally:
            s3_client.close()

    def upload_s3(
        self,
        file_key: str,
        local_file: str,
        delete_flag: bool = True,
        content_type: bool = False,
    ) -> Result:
        """
        Upload a local file to an S3 bucket and optionally delete the local file.

        :param file_key: The key (path) where the file will be stored in the S3 bucket.
        :type file_key: str
        :param local_file: The path to the local file to upload.
        :type local_file: str
        :param delete_flag: Whether to delete the local file after uploading. Defaults to True.
        :type delete_flag: bool
        :param content_type: Whether to set the content type for the file. Defaults to False.
        :type content_type: bool
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        try:
            self.logger.info(
                f"Uploading local file '{local_file}' to s3://{self.s3_bucket}/{file_key}..."
            )

            self.s3_upload_retry(
                file_key, local_file, self.s3_bucket, content_type, self.logger
            )

            if delete_flag:
                os.remove(local_file)
                self.logger.info(
                    f"Uploaded '{local_file}' to s3://{self.s3_bucket}/{file_key} and deleted local copy."
                )

            return Result(StatusCode.SUCCESS, "File uploaded to S3 successfully.")

        except Exception as e:
            error_msg = f"**** Error on -> S3Helpers -> upload_s3. Message: {str(e)}. Filename: {local_file}"
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)

    def create_bucket(self, bucket_name: str, region: Optional[str] = None) -> Result:
        """
        Create an S3 bucket with the specified name.

        :param bucket_name: The name of the S3 bucket to create.
        :type bucket_name: str
        :param region: The AWS region where the bucket should be created. Defaults to the class's region.
        :type region: Optional[str]
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        region = region or self.region_name

        try:
            self.logger.info(
                f"Creating S3 bucket '{bucket_name}' in region '{region}'..."
            )

            # Instantiate the S3 client
            s3_client = self._get_s3_client()

            try:
                if region == "us-east-1":
                    s3_client.create_bucket(Bucket=bucket_name)
                else:
                    s3_client.create_bucket(
                        Bucket=bucket_name,
                        CreateBucketConfiguration={"LocationConstraint": region},
                    )

                self.logger.info(f"S3 bucket '{bucket_name}' created successfully.")
                return Result(
                    StatusCode.SUCCESS, f"Bucket '{bucket_name}' created successfully."
                )

            finally:
                # Close the S3 client to release resources
                s3_client.close()

        except Exception as e:
            error_msg = (
                f"**** Error on -> S3Helpers -> create_bucket. Message: {str(e)}"
            )
            self.logger.error(error_msg)
            self.logger.error(traceback.format_exc())
            return Result(StatusCode.FAILURE, error_msg)
