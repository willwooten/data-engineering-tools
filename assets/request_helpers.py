# Standard Library Imports
import logging
import traceback
from typing import Any, Dict, Optional

# Third-Party Library Imports
import requests

# Local Application/Project Imports
from assets import retry_dec
from assets.utilities import Result, StatusCode, Utilities


class Requests:
    """
    A class for handling HTTP requests with retry and logging functionality.
    """

    def __init__(self, utils: Utilities):
        """
        Initialize the Requests class with utility functions and a logger.

        :param utils: An instance of the Utilities class for logging and helper functions.
        :type utils: Utilities
        """
        self.utils = utils
        self.logger = self.utils.logger

    def _response_logging(
        self,
        result: Result,
        url: str,
        payload: Optional[Dict] = None,
    ) -> None:
        """
        Log the response details and raise an exception if the result indicates failure.

        :param result: The result object containing the status code and message.
        :type result: Result
        :param url: The URL that was called.
        :type url: str
        :param payload: The request payload, if any.
        :type payload: Optional[Dict]
        :raises Exception: If the result status code is FAILURE.
        """
        self.logger.info(
            f"\nurl: {url}\npayload: {payload}\nresponse: {result.ret_msg}"
        )
        self.utils.check_raise(result)

    @retry_dec.retry(Exception, total_tries=2, initial_wait_secs=5)
    def request_retry(
        self,
        url: str,
        headers: Optional[Dict] = None,
        payload: Optional[Dict] = None,
        ret_type: str = "json",
        logger: Optional[logging.Logger] = None,
        method: str = "GET",
        cookies: Optional[Dict] = None,
    ) -> Any:
        """
        Make an HTTP request with retry logic.

        :param url: The URL to make the request to.
        :type url: str
        :param headers: The request headers.
        :type headers: Optional[Dict]
        :param payload: The request payload.
        :type payload: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param logger: A logger instance to use for retry logging.
        :type logger: Optional[logging.Logger]
        :param method: The HTTP method (e.g., 'GET', 'POST'). Defaults to 'GET'.
        :type method: str
        :param cookies: The request cookies.
        :type cookies: Optional[Dict]
        :return: The response data.
        :rtype: Any
        :raises Exception: If the request fails.
        """
        if headers is None:
            headers = {"Content-Type": "application/json"}

        response = requests.request(
            method, url, headers=headers, data=payload, cookies=cookies
        )

        if response.status_code == 200:
            if ret_type == "json":
                return response.json()
            else:
                return response.text
        else:
            raise Exception(f"{response.text} - {response.status_code}")

    def request_call(
        self,
        url: str,
        headers: Optional[Dict] = None,
        payload: Optional[Dict] = None,
        ret_type: str = "json",
        method: str = "GET",
        cookies: Optional[Dict] = None,
    ) -> Any:
        """
        Make an HTTP request and log the result.

        :param url: The URL to make the request to.
        :type url: str
        :param headers: The request headers.
        :type headers: Optional[Dict]
        :param payload: The request payload.
        :type payload: Optional[Any]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param method: The HTTP method (e.g., 'GET', 'POST'). Defaults to 'GET'.
        :type method: str
        :param cookies: The request cookies.
        :type cookies: Optional[Dict]
        :return: The response data.
        :rtype: Any
        :raises Exception: If the request fails.
        """
        try:
            # Uncomment the actual call when ready for production
            # data = self.request_retry(url, headers=headers, payload=payload, ret_type=ret_type, logger=self.utils.logger, method=method, cookies=cookies)
            data = {"result": {"block": "data"}}
            result = Result(StatusCode.SUCCESS, "Success", data)

        except Exception as e:
            error_msg = f"**** Error on -> constants -> request_call. Message: {str(e)}. Url: {url}"
            self.logger.error(error_msg)
            self.logger.error(str(traceback.format_exc()))
            result = Result(StatusCode.FAILURE, error_msg, None)

        self._response_logging(result, url, payload)
        return result.data

    def get(
        self,
        url: str,
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
    ) -> Any:
        """
        Make a GET request with retry and logging.

        :param url: The URL to make the GET request to.
        :type url: str
        :param headers: The request headers.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: The request cookies.
        :type cookies: Optional[Dict]
        :return: The response data.
        :rtype: Any
        :raises Exception: If the request fails.
        """
        return self.request_call(
            url, headers=headers, ret_type=ret_type, method="GET", cookies=cookies
        )

    def post(
        self,
        url: str,
        payload: Dict,
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
    ) -> Any:
        """
        Make a POST request with retry and logging.

        :param url: The URL to make the POST request to.
        :type url: str
        :param payload: The request payload.
        :type payload: Dict
        :param headers: The request headers.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: The request cookies.
        :type cookies: Optional[Dict]
        :return: The response data.
        :rtype: Any
        :raises Exception: If the request fails.
        """
        return self.request_call(
            url,
            headers=headers,
            payload=payload,
            ret_type=ret_type,
            method="POST",
            cookies=cookies,
        )
