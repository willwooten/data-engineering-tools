import logging
import asyncio
import backoff
from typing import Any, Callable, Dict, List, Optional
from aiohttp import ClientSession, TCPConnector
from assets.utilities import Utilities


class AsyncRequests:
    """
    A class to handle asynchronous HTTP GET and POST requests with batching support.
    """

    def __init__(self, utils: Utilities, limit: int = 100, limit_per_host: int = 0):
        """
        Initialize the AsyncRequests class with a logger and an HTTP client session.

        :param utils: A utilities instance containing a logger.
        :type utils: Utilities
        :param limit: The maximum number of concurrent connections.
        :type limit: int
        :param limit_per_host: The maximum number of concurrent connections per host.
        :type limit_per_host: int
        """
        self.utils = utils
        self.logger = self.utils.logger
        self.client = ClientSession(
            connector=TCPConnector(
                limit=limit, limit_per_host=limit_per_host, ssl=False
            ),
            raise_for_status=True,
        )

    def run_batch(self, request_type: str, batch: List[Dict]) -> None:
        """
        Run the asynchronous batch function within an event loop.

        :param request_type: The type of request ('GET' or 'POST').
        :type request_type: str
        :param batch: A list of dictionaries containing request details.
        :type batch: List[Dict]
        """
        asyncio.run(self._run_batch(request_type, batch))

    async def _run_batch(self, request_type: str, batch: List[Dict]) -> List:
        """
        Run a batch of GET or POST requests based on the specified request type.

        :param request_type: The type of request ('GET' or 'POST').
        :type request_type: str
        :param batch: A list of dictionaries containing request details.
        :type batch: List[Dict]
        :return: A list of responses or exceptions for each request.
        :rtype: List[Any]
        :raises ValueError: If the request type is not 'GET' or 'POST'.
        """
        if request_type.upper() == "GET":
            return await self._get_batch(batch)
        elif request_type.upper() == "POST":
            return await self._post_batch(batch)
        else:
            raise ValueError("Invalid request type. Please use 'GET' or 'POST'.")

    async def _get_batch(
        self,
        batch: List[Dict],
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
    ) -> List[Any]:
        """
        Perform a batch of GET requests concurrently.

        :param batch: A list of dictionaries containing 'url' keys for each GET request.
        :type batch: List[Dict]
        :param headers: Optional headers for the requests.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: Optional cookies for the requests.
        :type cookies: Optional[Dict]
        :return: A list of responses or exceptions for each request.
        :rtype: List[Any]
        """
        return await self._batch_requests(
            batch, self._get_async, headers, ret_type, cookies
        )

    async def _post_batch(
        self,
        batch: List[Dict],
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
    ) -> List[Any]:
        """
        Perform a batch of POST requests concurrently.

        :param batch: A list of dictionaries containing 'url' and 'payload' keys for each POST request.
        :type batch: List[Dict]
        :param headers: Optional headers for the requests.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: Optional cookies for the requests.
        :type cookies: Optional[Dict]
        :return: A list of responses or exceptions for each request.
        :rtype: List[Any]
        """
        return await self._batch_requests(
            batch, self._post_async, headers, ret_type, cookies
        )

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    async def _get_async(
        self,
        url: str,
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Any:
        """
        Perform a batch of HTTP requests concurrently using the specified method.

        :param batch: A list of dictionaries containing 'url' and optional 'payload' keys.
        :type batch: List[Dict]
        :param method: The asynchronous method to use for the requests (e.g., _get_async or _post_async).
        :type method: Callable
        :param headers: Optional headers for the requests.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: Optional cookies for the requests.
        :type cookies: Optional[Dict]
        :return: A list of responses or exceptions for each request.
        :rtype: List[Any]
        """
        if headers is None:
            headers = {"Content-Type": "application/json"}

        try:
            async with self.client.get(
                url, headers=headers, cookies=cookies
            ) as response:
                if response.status == 200:
                    if ret_type == "json":
                        return await response.json()
                    else:
                        return await response.text()
                else:
                    error_message = f"{await response.text()} - {response.status}"
                    if logger:
                        logger.error(error_message)
                    raise Exception(error_message)

        except Exception as e:
            if logger:
                logger.error(f"GET request failed: {e}")
            raise

    @backoff.on_exception(backoff.expo, Exception, max_tries=5)
    async def _post_async(
        self,
        url: str,
        headers: Optional[Dict] = None,
        payload: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
        logger: Optional[logging.Logger] = None,
    ) -> Any:
        """
        Perform an asynchronous POST request with retry logic.

        :param url: The URL to make the POST request to.
        :type url: str
        :param headers: Optional headers for the request.
        :type headers: Optional[Dict]
        :param payload: The payload to send in the POST request.
        :type payload: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: Optional cookies for the request.
        :type cookies: Optional[Dict]
        :param logger: Optional logger for logging errors.
        :type logger: Optional[logging.Logger]
        :return: The response data.
        :rtype: Any
        """
        if headers is None:
            headers = {"Content-Type": "application/json"}

        try:
            async with self.client.post(
                url, headers=headers, json=payload, cookies=cookies
            ) as response:
                if response.status == 200:
                    if ret_type == "json":
                        return await response.json()
                    else:
                        return await response.text()
                else:
                    error_message = f"{await response.text()} - {response.status}"
                    if logger:
                        logger.error(error_message)
                    raise Exception(error_message)

        except Exception as e:
            if logger:
                logger.error(f"POST request failed: {e}")
            raise

    async def _batch_requests(
        self,
        batch: List[Dict],
        method: Callable,
        headers: Optional[Dict] = None,
        ret_type: str = "json",
        cookies: Optional[Dict] = None,
    ) -> List[Any]:
        """
        Perform a batch of HTTP requests concurrently using the specified method.

        This function gathers multiple requests and executes them concurrently, handling errors
        individually for each request and logging failures without interrupting the entire batch.

        :param batch: A list of dictionaries containing request details.
                    Each dictionary should have a 'url' key and optionally a 'payload' key for POST requests.
        :type batch: List[Dict]
        :param method: The asynchronous method to use for the requests (e.g., _get_async or _post_async).
        :type method: Callable
        :param headers: Optional headers for the requests.
        :type headers: Optional[Dict]
        :param ret_type: The expected return type ('json' or 'text'). Defaults to 'json'.
        :type ret_type: str
        :param cookies: Optional cookies for the requests.
        :type cookies: Optional[Dict]
        :return: A list of responses or exceptions for each request.
        :rtype: List[Any]
        :raises Exception: If the entire batch fails unexpectedly.
        """
        try:
            results = await asyncio.gather(
                *[
                    method(
                        url=call["url"],
                        payload=call.get("payload"),
                        headers=headers,
                        ret_type=ret_type,
                        cookies=cookies,
                    )
                    for call in batch
                ],
                return_exceptions=True,  # Allow individual failures without affecting the whole batch
            )

            # Log errors if any request failed
            for i, result in enumerate(results):
                if isinstance(result, Exception):
                    self.logger.error(f"Request to {batch[i]['url']} failed: {result}")

            return results

        except Exception as e:
            self.logger.error(f"Batch request failed: {e}")
            raise
