# Standard Library Imports
import datetime
import json
import os
import time
import multiprocessing
from multiprocessing.pool import AsyncResult
from multiprocessing.process import BaseProcess
from typing import Any, Callable, Dict, List, Tuple

# Local Application/Project Imports
from assets.utilities import Result, StatusCode, Utilities


class PoolMonitor:
    """Monitors a pool of worker processes, logs progress, and handles timeouts."""

    DEFAULT_TIMEOUT_HOURS = 24
    SLEEP_SHORT = 10
    SLEEP_LONG = 60

    def __init__(
        self,
        utils: Utilities,
        status_dict: Dict[str, Dict[str, str]],
        timeout_hours: int = DEFAULT_TIMEOUT_HOURS,
    ):
        """
        Initialize the PoolMonitor with a Utilities instance and tracking state.

        :param utils: Instance of Utilities for logging and utility functions.
        :type utils: Utilities
        :param status_dict: A dictionary representing the current status of the pool.
        :type status_dict: Dict[str, Dict[str, str]]
        :param timeout_hours: The maximum number of hours to wait before timing out.
        :type timeout_hours: int
        """
        self.utils = utils
        self.logger = utils.logger
        self.tracking = self._initialize_tracking(status_dict)
        self.timeout_hours = timeout_hours

    def _initialize_tracking(
        self, status_dict: Dict[str, Dict[str, str]]
    ) -> Dict[str, Any]:
        """
        Initialize tracking values for monitoring the pool.

        :param status_dict: A dictionary representing the current status of the pool.
        :type status_dict: Dict[str, Dict[str, str]]
        :return: A dictionary with initialized tracking values.
        :rtype: Dict[str, Any]
        """
        return {
            "start_time": time.time(),
            "same_count": 0,
            "data_load_wait_count": 0,
            "overall_wait_count": 0,
            "previous_pending": -1,
            "prev_status_dict": status_dict.copy(),
        }

    def _increment_tracking_counters(self) -> None:
        """Increment the tracking counters for data load and overall wait counts."""
        self.tracking["data_load_wait_count"] += 1
        self.tracking["overall_wait_count"] += 1

    def _check_timeout(self, active_children: List[BaseProcess]) -> bool:
        """
        Check if the monitoring has exceeded the timeout limit.

        :param active_children: List of active worker processes.
        :type active_children: List[BaseProcess]
        :return: True if timeout is reached, False otherwise.
        :rtype: bool
        """
        elapsed_time = (time.time() - self.tracking["start_time"]) / 3600  # in hours
        if elapsed_time > self.timeout_hours:
            self.logger.error(
                f"Timeout of {self.timeout_hours} hours reached. Terminating processes."
            )
            for worker in active_children:
                if worker.is_alive():
                    self.logger.warning(
                        f"Terminating worker {worker.name} (PID: {worker.pid})"
                    )
                    worker.terminate()
            return True
        return False

    def _check_pool_status(
        self, pool_results: List[Tuple[str, AsyncResult]]
    ) -> Tuple[List[str], int, int, int]:
        """
        Check the status of pool results and categorize tasks as pending, started, or failed.

        :param pool_results: List of tuples containing child IDs and their AsyncResult objects.
        :type pool_results: List[Tuple[str, AsyncResult]]
        :return: A tuple containing the list of pending task messages, and counts of pending, started, and failed tasks.
        :rtype: Tuple[List[str], int, int, int]
        """
        pending_tasks = []
        pending_count = started_count = failed_count = 0

        for child_id, result in pool_results:
            try:
                if result.successful():
                    started_count += 1
                else:
                    failed_count += 1
                    self.logger.error(f"Task with ID '{child_id}' failed.")
            except Exception as e:
                pending_count += 1
                pending_tasks.append(
                    f"Task with ID '{child_id}' is pending due to: {e}"
                )

        return pending_tasks, pending_count, started_count, failed_count

    def _log_worker_status(self, active_children: List[BaseProcess]) -> None:
        """
        Log the status of active worker processes.

        :param active_children: List of active worker processes.
        :type active_children: List[BaseProcess]
        """
        for worker in active_children:
            self.logger.info(
                f"Worker {worker.name} (PID: {worker.pid}) - Alive: {worker.is_alive()}"
            )

    def _log_status_message(
        self, tag: str, max_count: int, pending: int, started: int, failed: int
    ) -> None:
        """
        Log the current status message with rate and estimated remaining time.

        :param tag: A tag to identify the pool monitoring session.
        :type tag: str
        :param max_count: The maximum number of tasks to monitor.
        :type max_count: int
        :param pending: Count of pending tasks.
        :type pending: int
        :param started: Count of started tasks.
        :type started: int
        :param failed: Count of failed tasks.
        :type failed: int
        """
        elapsed_time = max(time.time() - self.tracking["start_time"], 1)
        rate = max((started + failed) / elapsed_time, 0.1)
        estimated_time_left = time.strftime("%H:%M:%S", time.gmtime(pending / rate))

        self.logger.info(
            f"{tag}: Total: {max_count}, Pending: {pending}, "
            f"Completed: {started}, Failed: {failed}, "
            f"Rate: {round(rate, 3)} tasks/sec, Estimated Time Left: {estimated_time_left}"
        )

    def _check_for_killed_workers(self, active_children: List[BaseProcess]) -> bool:
        """
        Check if any worker processes have been terminated.

        :param active_children: List of active worker processes.
        :type active_children: List[BaseProcess]
        :return: True if any worker process is no longer alive, False otherwise.
        :rtype: bool
        """
        return any(not worker.is_alive() for worker in active_children)

    def _dynamic_sleep(self) -> None:
        """Sleep dynamically based on the overall wait count."""
        sleep_time = (
            self.SLEEP_SHORT
            if self.tracking["overall_wait_count"] <= 10
            else self.SLEEP_LONG
        )
        time.sleep(sleep_time)

    def monitor_pool(
        self,
        active_children: List[BaseProcess],
        pool_results: List[Tuple[str, AsyncResult]],
        tag: str,
        max_count: int,
    ) -> Result:
        """
        Monitor a pool of worker processes, log progress, and handle timeouts.

        :param active_children: List of active worker processes.
        :type active_children: List[BaseProcess]
        :param pool_results: List of tuples containing child IDs and their AsyncResult objects.
        :type pool_results: List[Tuple[str, AsyncResult]]
        :param tag: A tag to identify the pool monitoring session.
        :type tag: str
        :param max_count: The maximum number of tasks to monitor.
        :type max_count: int
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        self._log_worker_status(active_children)

        while True:
            self._increment_tracking_counters()

            pending_tasks, pending, started, failed = self._check_pool_status(
                pool_results
            )
            self._log_status_message(tag, max_count, pending, started, failed)

            if pending_tasks:
                self.logger.debug(
                    f"Pending tasks:\n{json.dumps(pending_tasks, indent=2)}"
                )

            if self._check_for_killed_workers(active_children):
                self.logger.error(
                    "Some pool members were killed and did not complete on time."
                )
                return Result(
                    StatusCode.FAILURE,
                    "Some pool members were killed and did not complete on time.",
                    [],
                )

            if self._check_timeout(active_children):
                return Result(
                    StatusCode.FAILURE,
                    f"Monitoring timed out after {self.timeout_hours} hours.",
                    [],
                )

            if pending == 0:
                break

            self.tracking["previous_pending"] = pending
            self._dynamic_sleep()

        return Result(StatusCode.SUCCESS, "Pools finished successfully.")


class PoolProcesses:
    """
    A class to manage multiprocessing pool operations and batch processing.

    :param utils: An instance of the Utilities class for logging and helper functions.
    :type utils: Utilities
    """

    def __init__(self, utils: Utilities):
        """
        Initialize the PoolProcesses class with utilities, logger, and default settings.

        :param utils: An instance of the Utilities class for logging and configuration.
        :type utils: Utilities
        """
        self.utils = utils
        self.logger = self.utils.logger
        self.workers = os.cpu_count()
        self.batchsize = 100

    @staticmethod
    def assign_status(
        new_val_dict: Dict[str, Any], status_dict: Dict[str, Dict[str, Any]], key: str
    ) -> Dict[str, Dict[str, Any]]:
        """
        Update the status dictionary with new values for a given key.

        Since updating mutable objects produced by `multiprocessing.Manager().dict` can cause issues,
        this method reassigns the dictionary to apply changes.

        :param new_val_dict: A dictionary containing new status values to assign.
        :type new_val_dict: Dict[str, Any]
        :param status_dict: The status dictionary to be updated.
        :type status_dict: Dict[str, Dict[str, Any]]
        :param key: The key in the status dictionary to update.
        :type key: str
        :return: The updated status dictionary.
        :rtype: Dict[str, Dict[str, Any]]
        """
        # Create a new dictionary to hold the updated values
        mod_dict = {}
        for k, v in status_dict[key].items():
            mod_dict[k] = v  # Existing
        for k, v in new_val_dict.items():
            mod_dict[k] = v  # New
        status_dict[key] = mod_dict

    def status_check(self, status_dict: Dict[str, Dict[str, str]]) -> None:
        """
        Check the status of tasks in the status dictionary and log the results.

        This function iterates through the status dictionary, logs successes and failures,
        and raises an exception if any task has failed.

        :param status_dict: A dictionary containing task statuses with keys representing task names.
                            Each value is a dictionary with keys 'STATUS' and 'EXCEPTION'.
        :type status_dict: Dict[str, Dict[str, str]]
        :raises Exception: If any task has failed, raises an exception with the first encountered error message.
        """
        success, failed, first_error = 0, 0, ""
        for k, v in status_dict.items():
            if v["STATUS"] != "COMPLETED":
                if first_error == "":
                    first_error = f"{k} status: {v['STATUS']}: {v['EXCEPTION']}"
                self.logger.error(
                    f"{k} status: {v['STATUS']} with error message: {v['EXCEPTION']}"
                )
                failed += 1
            else:
                success += 1
                self.logger.info(f"{k} Succeeded")

        self.logger.info(
            f":::::::::: Out of {len(status_dict)}, {success} succeeded and {failed} failed ::::::::::"
        )

        if failed != 0:
            raise Exception(first_error)

    def pool_wait(
        self,
        active_children: List[BaseProcess],
        pool_results: List[Tuple[str, AsyncResult]],
        tag: str,
        max_count: int,
        status_dict: Dict[str, Dict[str, str]],
        **kwargs: Dict[str, Any],
    ) -> Result:
        """
        Monitor a pool of processes and wait for their completion.

        :param active_children: List of active child processes.
        :type active_children: List[BaseProcess]
        :param pool_results: List of tuples containing task IDs and their AsyncResult objects.
        :type pool_results: List[Tuple[str, AsyncResult]]
        :param tag: A tag to identify the current pool operation.
        :type tag: str
        :param max_count: The maximum number of tasks expected.
        :type max_count: int
        :param status_dict: Dictionary to track the status of tasks.
        :type status_dict: Dict[str, Dict[str, str]]
        :param kwargs: Additional keyword arguments for timeout and retry options.
        :type kwargs: Any
        :return: A Result object indicating success or failure.
        :rtype: Result
        """
        monitor = PoolMonitor(self.utils, status_dict)
        return monitor.monitor_pool(
            active_children, pool_results, tag, max_count, **kwargs
        )

    def run_pool_processes(
        self, call_function: Callable, total_range: List[Dict]
    ) -> None:
        """
        Execute a pool of processes to run a specified function in parallel over a range of tasks.

        This function sets up a multiprocessing pool and assigns batches of tasks to the pool workers.
        Each task's status is tracked using a manager dictionary. After all tasks are submitted, the function
        waits for the pool processes to complete and checks their statuses.

        :param call_function: The function to be executed in parallel by the pool processes.
                            This function should accept the following arguments:
                            `key`, `call_id`, `batch_start`, `block_number`, `status_dict`, and `target_url`.
        :type call_function: Callable
        :param total_range: A list of dictionaries where each dictionary represents a task. Each task should
                            contain the following keys: `call_id`, `endpoint`, and `block_number`.
        :type total_range: List[Dict]
        :raises Exception: If there are issues during pool execution or status checks.
        """
        pool = multiprocessing.Pool(processes=self.workers)
        manager = multiprocessing.Manager()

        pool_results = []
        status_dict = manager.dict()
        max_count = len(total_range) + 1
        batch = 1

        for batch_start in range(1, max_count, self.batchsize):

            batched_calls = total_range[
                batch_start - 1 : batch_start + self.batchsize - 1
            ]

            for call in batched_calls:

                call_id = call.get("call_id")
                key = f"{batch}.{call_id}"
                target_url = call.get("endpoint")
                block_number = call.get("block_number")

                status_dict[key] = {
                    "STATUS": "NOT_STARTED",
                    "STARTED_AT": datetime.datetime.now(),
                }
                p = pool.apply_async(
                    call_function,
                    [key, call_id, batch_start, block_number, status_dict, target_url],
                )

                pool_results.append((key, p))
            batch += 1

        active_children = multiprocessing.active_children()

        result = self.pool_wait(
            active_children,
            pool_results,
            "ethereum-block-call",
            max_count,
            status_dict,
        )

        self.logger.info(
            f"Parent level pool exit code: {result.ret_cd} and msg: {result.ret_msg}. Checking extract/upload status..."
        )

        self.status_check(status_dict)
