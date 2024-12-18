# Standard Library Imports
import random
import time
from functools import wraps
from typing import Any, Callable, List, Optional, Tuple, Type


def get_args(args: Optional[Tuple[Any, ...]]) -> str:
    """
    Return the provided arguments or a default message if no arguments are given.

    :param args: The arguments to be processed.
    :type args: Optional[Tuple[Any, ...]]
    :return: The arguments if provided, otherwise 'no input arguments'.
    :rtype: str
    """
    return str(args) if args else "no input arguments"


def check_known_exception(
    exception_list: Optional[List[str]], exception_message: str
) -> bool:
    """
    Check if the exception message matches any known exceptions from the list.

    If the list is None, the function returns True (retry allowed). Otherwise,
    it checks if the exception message contains any known exceptions.

    :param exception_list: A list of known exception messages to check against.
    :type exception_list: Optional[List[str]]
    :param exception_message: The exception message to check.
    :type exception_message: str
    :return: True if the exception message matches a known exception or the list is None, otherwise False.
    :rtype: bool
    """
    if exception_list is None:
        return True

    for known_exception in exception_list:
        if known_exception in exception_message:
            return True

    return False


def check_no_retry_exception(
    exception_list: Optional[List[str]], exception_message: str
) -> bool:
    """
    Check if the exception message matches any no-retry exceptions from the list.

    If the list is None, the function returns True (retry not allowed). Otherwise,
    it checks if the exception message contains any no-retry exceptions.

    :param exception_list: A list of no-retry exception messages to check against.
    :type exception_list: Optional[List[str]]
    :param exception_message: The exception message to check.
    :type exception_message: str
    :return: False if the exception message matches a no-retry exception, otherwise True.
    :rtype: bool
    """
    if exception_list is None:
        return True

    for no_retry_exception in exception_list:
        if no_retry_exception in exception_message:
            return False

    return True


def retry(
    exceptions: Type[Exception],
    total_tries: int = 10,
    initial_wait_secs: int = 2,
    back_off_factor: int = 2,
    exception_list: Optional[List[str]] = None,
    no_retry_list: Optional[List[str]] = None,
) -> Callable[[Callable[..., Any]], Callable[..., Any]]:
    """
    Decorator for retrying a function in case of an exception.

    :param exceptions: The exceptions to trigger a retry.
    :type exceptions: Type[Exception]
    :param total_tries: The total number of retry attempts. Defaults to 10.
    :type total_tries: int
    :param initial_wait_secs: The time to wait before the first retry in seconds. Defaults to 2.
    :type initial_wait_secs: int
    :param back_off_factor: The back-off multiplier for increasing the wait time. Defaults to 2.
    :type back_off_factor: int
    :param exception_list: A list of known exceptions to allow retrying. If None, retries for all exceptions.
    :type exception_list: Optional[List[str]]
    :param no_retry_list: A list of exceptions that should not trigger a retry.
    :type no_retry_list: Optional[List[str]]
    :return: A decorator that adds retry logic to a function.
    :rtype: Callable[[Callable[..., Any]], Callable[..., Any]]
    """

    def retry_decorator(f):
        @wraps(f)
        def func_with_retries(*args, **kwargs):
            _tries, _delay = total_tries + 1, initial_wait_secs
            logger = kwargs.get("logger", None)

            while _tries >= 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    exception_message = str(e)
                    retry_flag = True

                    if exception_list:
                        retry_flag = check_known_exception(
                            exception_list, exception_message
                        )

                    if no_retry_list:
                        retry_flag = check_no_retry_exception(
                            no_retry_list, exception_message
                        )

                    if retry_flag:
                        print_args = get_args(args)

                        if _tries == 1:
                            content = (
                                f"\nFunction: {f.__name__}\n"
                                f"Exception: {exception_message}\n"
                                f"Failed after {total_tries} retries with args: {print_args} and kwargs: {kwargs}\n"
                            )
                            if logger:
                                logger.error(content)
                            else:
                                print(content)
                            raise

                        content = (
                            f"\nRetry {total_tries + 2 - _tries}\n"
                            f"Function: {f.__name__}\n"
                            f"Exception: {exception_message}\n"
                            f"Retrying in {_delay} seconds with args: {print_args} and kwargs: {kwargs}\n"
                        )
                        if logger:
                            logger.warning(content)
                        else:
                            print(content)

                        time.sleep(_delay)
                        _delay *= back_off_factor + random.randint(1, 9)
                        _tries -= 1
                    else:
                        raise

        return func_with_retries

    return retry_decorator
