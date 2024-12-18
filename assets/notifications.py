import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
import requests
from assets.utilities import Result, StatusCode, Utilities


class NotificationController:
    """
    A class to send notifications via Email, Slack, and Telegram.
    """

    def __init__(
        self,
        utils: Utilities,
        email_config: dict,
        slack_token: str = None,
        telegram_token: str = None,
    ):
        """
        Initialize the NotificationController with configurations.

        :param utils: An instance of the Utilities class for logging.
        :param email_config: Configuration dictionary for email (SMTP).
        :param slack_token: Slack Bot User OAuth Token.
        :param telegram_token: Telegram Bot Token.
        """
        self.utils = utils
        self.logger = self.utils.logger

        # Email Config
        self.smtp_server = email_config.get("smtp_server")
        self.smtp_port = email_config.get("smtp_port")
        self.sender_email = email_config.get("sender_email")
        self.sender_password = email_config.get("sender_password")

        # Slack Config
        self.slack_client = WebClient(token=slack_token) if slack_token else None

        # Telegram Config
        self.telegram_token = telegram_token

    def send_email(self, recipient_email: str, subject: str, body: str) -> Result:
        """
        Send an email notification.

        :param recipient_email: Recipient's email address.
        :param subject: Subject of the email.
        :param body: Body of the email.
        :return: Result object indicating success or failure.
        """
        try:
            msg = MIMEMultipart()
            msg["From"] = self.sender_email
            msg["To"] = recipient_email
            msg["Subject"] = subject

            msg.attach(MIMEText(body, "plain"))

            with smtplib.SMTP(self.smtp_server, self.smtp_port) as server:
                server.starttls()
                server.login(self.sender_email, self.sender_password)
                server.sendmail(self.sender_email, recipient_email, msg.as_string())

            self.logger.info(
                f"Email sent to {recipient_email} with subject '{subject}'."
            )
            return Result(StatusCode.SUCCESS, "Email sent successfully.")
        except Exception as e:
            error_msg = f"Failed to send email: {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def send_slack_message(self, channel: str, message: str) -> Result:
        """
        Send a Slack message to a channel.

        :param channel: Slack channel (e.g., '#general') or user ID.
        :param message: The message text.
        :return: Result object indicating success or failure.
        """
        if not self.slack_client:
            error_msg = "Slack token not provided."
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

        try:
            response = self.slack_client.chat_postMessage(channel=channel, text=message)
            self.logger.info(f"Slack message sent to {channel}: '{message}'")
            return Result(
                StatusCode.SUCCESS,
                "Slack message sent successfully.",
                data=response.data,
            )
        except SlackApiError as e:
            error_msg = f"Failed to send Slack message: {e.response['error']}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

    def send_telegram_message(self, chat_id: str, message: str) -> Result:
        """
        Send a Telegram message to a chat.

        :param chat_id: The chat ID or username to send the message to.
        :param message: The message text.
        :return: Result object indicating success or failure.
        """
        if not self.telegram_token:
            error_msg = "Telegram token not provided."
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)

        url = f"https://api.telegram.org/bot{self.telegram_token}/sendMessage"
        payload = {"chat_id": chat_id, "text": message}

        try:
            response = requests.post(url, json=payload)
            response.raise_for_status()
            self.logger.info(f"Telegram message sent to {chat_id}: '{message}'")
            return Result(
                StatusCode.SUCCESS,
                "Telegram message sent successfully.",
                data=response.json(),
            )
        except requests.RequestException as e:
            error_msg = f"Failed to send Telegram message: {str(e)}"
            self.logger.error(error_msg)
            return Result(StatusCode.FAILURE, error_msg)
