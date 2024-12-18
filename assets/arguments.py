# Standard Library Imports
import argparse
from dataclasses import dataclass
from typing import Dict, List, Literal, Optional


@dataclass
class UserInput:
    """
    Represents the user input for the application.

    :param target: The target of the job, e.g., 'blocks'.
    :type target: str
    :param start_block: The starting block number (default is None).
    :type start_block: Optional[int]
    :param end_block: The ending block number (default is None).
    :type end_block: Optional[int]
    """

    target: str
    start_block: Optional[int]
    end_block: Optional[int]


@dataclass
class S3Config:
    s3_bucket: str = "development"
    region_name: str = "us-east-1"
    aws_access_key_id: str = "test"
    aws_secret_access_key: str = "test"


@dataclass
class Arguments:
    """
    Represents the arguments for running a job in the application.

    :param job_name: The name of the job to execute.
    :type job_name: str
    :param job_run_id: A unique identifier for the job run.
    :type job_run_id: str
    :param protocol_cd: The protocol code, e.g., 'ETHEREUM' or 'OPTIMISM'.
    :type protocol_cd: Literal["ETHEREUM", "OPTIMISM"]
    :param user_input: The user input parameters associated with the job.
    :type user_input: UserInput
    :param target_calls_filename: The optional filename for target calls.
    :type target_calls_filename: Optional[str]
    :param s3_bucket: The name of the S3 bucket where data will be stored or retrieved.
    :type s3_bucket: Optional[str]
    """

    job_name: str
    job_run_id: str
    protocol_cd: Literal["ETHEREUM", "OPTIMISM"]
    user_input: UserInput
    target_calls_filename: Optional[str]
    s3_config: S3Config


class ArgumentHandler:
    """
    Handles parsing of command-line arguments for the application.

    :param args: The list of arguments passed to the command line.
    :type args: List[str]
    """

    def __init__(self, args: List[str]) -> None:
        """
        Initializes the ArgumentHandler with the provided arguments.

        :param args: The list of command-line arguments.
        :type args: List[str]
        """
        self.args = args
        self.parser = argparse.ArgumentParser(
            description="Main entry point for the app process."
        )
        self._add_arguments()

    def _add_arguments(self) -> None:
        """Adds expected arguments to the parser."""
        self.parser.add_argument(
            "-j",
            "--job_name",
            help="Input job name.",
            default="amt_adapt_bfl_refinitiv_history_load_t",
        )
        self.parser.add_argument(
            "-n", "--job_run_id", help="Job run ID.", default="20240422-234402"
        )
        self.parser.add_argument(
            "-v",
            "--protocol_cd",
            help="Choose the protocol code.",
            choices=["ETHEREUM", "OPTIMISM"],
            default="OPTIMISM",
        )
        self.parser.add_argument(
            "-u",
            "--user_input",
            help="Application-related input in the format 'target=blocks|start_block=1000|end_block=1001'.",
            default="target=blocks|start_block=1000|end_block=1001",
        )
        self.parser.add_argument(
            "-tcf",
            "--target_calls_filename",
            help="Location of file containing call targets.",
            default="target_calls.json",
        )
        self.parser.add_argument(
            "-b",
            "--bucket",
            help="Landing S3 Bucket",
            default="development",
        )

    def load_arguments(self) -> Arguments:
        """
        Parses the command-line arguments and returns them as an Arguments instance.

        :return: The parsed arguments encapsulated in an Arguments instance.
        :rtype: Arguments
        """
        return self._parse_arguments()

    def _parse_arguments(self) -> Arguments:
        """
        Parses the command-line arguments and converts them to an Arguments instance.

        :return: The parsed arguments.
        :rtype: Arguments
        """
        arg_namespace = self.parser.parse_args(self.args[1:])
        return self._namespace_to_dataclass(arg_namespace)

    def _namespace_to_dataclass(self, args: argparse.Namespace) -> Arguments:
        """
        Converts the argparse namespace to an Arguments dataclass instance.

        :param args: The namespace containing parsed arguments.
        :type args: argparse.Namespace
        :return: The arguments encapsulated in an Arguments instance.
        :rtype: Arguments
        """
        return Arguments(
            job_name=args.job_name,
            job_run_id=args.job_run_id,
            protocol_cd=args.protocol_cd,
            user_input=self._parse_user_input(args.user_input),
            target_calls_filename=args.target_calls_filename,
            s3_config=S3Config(s3_bucket=args.bucket),
        )

    def _parse_user_input(self, user_input: str) -> UserInput:
        """
        Parses the user input string into a UserInput dataclass.

        The input format is expected to be:
        "target=blocks|start_block=1000|end_block=1001"

        :param user_input: The user input string to parse.
        :type user_input: str
        :return: The parsed user input encapsulated in a UserInput instance.
        :rtype: UserInput
        :raises ValueError: If the target is not provided in the user input.
        """
        try:
            inputs: Dict[str, str] = dict(
                item.split("=") for item in user_input.split("|")
            )
        except ValueError as e:
            raise ValueError(f"Invalid user input format: {user_input}") from e

        target = inputs.get("target")
        if not target:
            raise ValueError(
                "You must provide a 'target' in user inputs (e.g., target=blocks)."
            )

        return UserInput(
            target=target,
            start_block=int(inputs.get("start_block", 0)),
            end_block=int(inputs.get("end_block", 0)),
        )
