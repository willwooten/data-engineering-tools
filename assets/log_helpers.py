# Standard Library Imports
import logging
from typing import Optional

# Local Application/Project Imports
from assets.arguments import Arguments


class ApplicationLogging:
    """
    A class to set up and manage logging for protocol-specific jobs.

    :param args: The parsed arguments containing job details and protocol information.
    :type args: Arguments
    """

    def __init__(self, args: Arguments):
        """
        Initialize the ApplicationLogging class with the provided arguments.

        :param args: The parsed arguments containing job details and protocol information.
        :type args: Arguments
        """
        self.args = args
        self.logger = self._setup_logger()

    def _setup_logger(
        self,
        log_component: Optional[str] = None,
        tag: Optional[str] = None,
        log_level: Optional[str] = None,
    ) -> logging.Logger:
        """
        Set up and return a logger for the specified component.

        :param log_component: The name of the logging component. Defaults to protocol code.
        :type log_component: Optional[str]
        :param tag: An optional tag to append to the log component name.
        :type tag: Optional[str]
        :param log_level: The logging level (e.g., 'INFO', 'DEBUG'). Defaults to 'INFO'.
        :type log_level: Optional[str]
        :return: A configured logger instance.
        :rtype: logging.Logger
        """
        log_component = log_component or self.args.protocol_cd
        logger_name = f"{log_component}_{tag}" if tag else log_component

        logger = logging.getLogger(logger_name)

        if not logger.hasHandlers():
            level = log_level or getattr(self, "log_level", "INFO")
            logger.setLevel(getattr(logging, level.upper(), logging.INFO))
            handler = logging.StreamHandler()
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(filename)s ::: %(funcName)s ::: %(message)s"
            )
            handler.setFormatter(formatter)
            logger.addHandler(handler)

        logger.info(f"==================== {logger_name} ====================")
        return logger

    def log_job_details(self) -> None:
        """
        Log the details of the current job.
        """
        self.log_title_box(
            f"Running with args => "
            f"job_name: {self.args.job_name}, "
            f"job_run_id: {self.args.job_run_id}, "
            f"protocol_cd: {self.args.protocol_cd}, "
            f"user_input: {self.args.user_input}"
        )

    def log_title_box(self, title: str) -> None:
        """
        Log a message with a decorative title box to highlight important sections.

        :param title: The title text to display inside the box.
        :type title: str
        """
        message = f" {title} "
        border_length = len(message)
        top_bottom_border = f"╔{'═' * border_length}╗"
        middle_border = f"║{message}║"

        self.logger.info(f"\n{top_bottom_border}\n{middle_border}\n{top_bottom_border}")
