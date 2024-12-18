# Standard Library Imports
import sys
import time
import traceback
from typing import List

# Local Application/Project Imports
from assets.utilities import Result, StatusCode, Utilities
from protocols import ethereum


class MainApplication:
    def __init__(self, args: List[str], localstack: bool = False):
        self.utils: Utilities = Utilities(args)
        self.target_calls = self.utils.load_target_calls()
        self.localstack = localstack

    def process(self) -> Result:
        """
        Process the job based on the protocol code specified in the arguments.

        Logs job details, attempts to start the protocol-specific process,
        and handles exceptions gracefully by logging errors and returning a failure result.

        :return: The result of the process execution indicating success or failure.
        :rtype: Result
        """
        self.utils.logger.info("Starting job processing...")
        self.utils.log_helper.log_job_details()

        try:
            result = self._protocol_start()
        except Exception as e:
            error_msg = f"Main module failed: {str(e)}"
            self.utils.logger.error(error_msg)
            self.utils.logger.error(traceback.format_exc())
            self.utils.logger.info("Process ended with failure.")
            return Result(StatusCode.FAILURE, error_msg)

        if result.ret_cd == StatusCode.FAILURE:
            self.utils.logger.warning("Job processing completed with failures.")
        else:
            self.utils.logger.info("Job processing completed successfully.")

        return result

    def _protocol_start(self) -> Result:
        protocol_dispatch = {
            "ETHEREUM": lambda: ethereum.EthereumClass(
                self.utils, localstack=self.localstack
            ).start_ethereum(target_calls=self.target_calls),
            # Add other protocols here when implemented, e.g.:
            # "OPTIMISM": lambda: optimism.OptimismClass(self.utils).start_optimism(),
        }

        protocol_cd = self.utils.args.protocol_cd.upper()

        if protocol_cd in protocol_dispatch:
            self.utils.log_helper.log_title_box(f"Starting {protocol_cd} process")
            return protocol_dispatch[protocol_cd]()

        error_msg = f"Protocol code '{protocol_cd}' is not implemented."
        self.utils.logger.error(error_msg)
        return Result(StatusCode.FAILURE, error_msg)


def main():
    try:
        app = MainApplication(sys.argv, localstack=False)
        result = app.process()
    except Exception as e:
        result = Result(StatusCode.FAILURE, f"Exception occurred: {str(e)}")
        app.utils.logger.error(traceback.format_exc())
    finally:
        final_emoji = (
            "\U0001f600" if result.ret_cd == StatusCode.SUCCESS else "\U0001f61f"
        )
        app.utils.logger.info(
            f'{time.strftime("%Y-%m-%d %H:%M:%S")} =>>>> {final_emoji} Final call at main with code: {result.ret_cd} and message: {result.ret_msg} <<<==='
        )
    return result.ret_cd


if __name__ == "__main__":
    exit(main())
    # python main.py \
    # -j dummy-job-name \
    # -n 2024.12.15.10.02.55.542 \
    # -b "development" \
    # -v ETHEREUM \
    # -tcf target_calls_1000.json \
    # -u "target=blocks|start_block=125|end_block=694"
