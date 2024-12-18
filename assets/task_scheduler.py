import schedule
import time
from threading import Thread
from typing import Callable, Any
from assets.utilities import Utilities


class TaskScheduler:
    """
    A class to manage scheduling and running periodic tasks.
    """

    def __init__(self, utils: Utilities):
        """
        Initialize the TaskScheduler with utility functions for logging.

        :param utils: An instance of the Utilities class for logging.
        :type utils: Utilities
        """
        self.utils = utils
        self.logger = self.utils.logger
        self.jobs = []

    def schedule_task(
        self, interval: int, unit: str, task: Callable, *args: Any, **kwargs: Any
    ) -> None:
        """
        Schedule a task to run periodically.

        :param interval: The frequency of the task.
        :type interval: int
        :param unit: The time unit ('seconds', 'minutes', 'hours', 'days', 'weeks').
        :type unit: str
        :param task: The function to be scheduled.
        :type task: Callable
        :param args: Positional arguments to pass to the task function.
        :param kwargs: Keyword arguments to pass to the task function.
        """
        unit_map = {
            "seconds": schedule.every(interval).seconds,
            "minutes": schedule.every(interval).minutes,
            "hours": schedule.every(interval).hours,
            "days": schedule.every(interval).days,
            "weeks": schedule.every(interval).weeks,
        }

        if unit not in unit_map:
            self.logger.error(
                f"Invalid scheduling unit '{unit}'. Supported units: seconds, minutes, hours, days, weeks."
            )
            return

        job = unit_map[unit].do(task, *args, **kwargs)
        self.jobs.append(job)
        self.logger.info(f"Scheduled task '{task.__name__}' every {interval} {unit}.")

    def schedule_task_at_time(
        self, time_str: str, task: Callable, *args: Any, **kwargs: Any
    ) -> None:
        """
        Schedule a task to run at a specific time daily.

        :param time_str: The time in 'HH:MM' format.
        :type time_str: str
        :param task: The function to be scheduled.
        :type task: Callable
        :param args: Positional arguments to pass to the task function.
        :param kwargs: Keyword arguments to pass to the task function.
        """
        job = schedule.every().day.at(time_str).do(task, *args, **kwargs)
        self.jobs.append(job)
        self.logger.info(f"Scheduled task '{task.__name__}' at {time_str} daily.")

    def cancel_all_tasks(self) -> None:
        """
        Cancel all scheduled tasks.
        """
        schedule.clear()
        self.jobs.clear()
        self.logger.info("All scheduled tasks have been cancelled.")

    def run_scheduler(self) -> None:
        """
        Run the scheduler in a separate thread to avoid blocking the main thread.
        """

        def run():
            self.logger.info("Task scheduler started.")
            while True:
                schedule.run_pending()
                time.sleep(1)

        Thread(target=run, daemon=True).start()
        self.logger.info("Task scheduler thread initiated.")

    def list_scheduled_tasks(self) -> None:
        """
        List all currently scheduled tasks.
        """
        if not self.jobs:
            self.logger.info("No tasks are currently scheduled.")
            return

        self.logger.info("Currently scheduled tasks:")
        for job in self.jobs:
            self.logger.info(str(job))


"""
from assets.utilities import Utilities
from assets.task_scheduler import TaskScheduler
import time


def example_task():
    print(f"Task executed at {time.strftime('%Y-%m-%d %H:%M:%S')}")


def another_task(name):
    print(f"Hello, {name}! Task executed at {time.strftime('%Y-%m-%d %H:%M:%S')}")


def main():
    utils = Utilities()
    scheduler = TaskScheduler(utils)

    # Schedule a task every 5 seconds
    scheduler.schedule_task(5, 'seconds', example_task)

    # Schedule a task every 2 minutes with arguments
    scheduler.schedule_task(2, 'minutes', another_task, name="Alice")

    # Schedule a task to run at a specific time (e.g., 14:30 daily)
    scheduler.schedule_task_at_time("14:30", example_task)

    # Start the scheduler
    scheduler.run_scheduler()

    # Keep the main thread alive
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        scheduler.cancel_all_tasks()
        print("Scheduler stopped.")


if __name__ == "__main__":
    main()


"""
