""" Long-running async task runner """

import logging
from datetime import timedelta
from threading import Event, Thread
from typing import List, Callable
import signal

from django.core.management.base import BaseCommand, CommandError, CommandParser
from django.conf import settings

from dbqueue.runner.postgres_notifications import NotificationThread
from dbqueue.runner.thread_helpers import WaitEvent
from dbqueue.runner.wakeup import Wakeup
from dbqueue.runner.jobs import JobRunner


# Allow Postgres and PostGIS
NOTIFY_ENGINES = (
    "django.contrib.gis.db.backends.postgis",
    "django.db.backends.postgresql",
    "django.db.backends.postgresql_psycopg2",
)

# Mapping between Django connection argument names,
# and the args that should be passed to psycopg2.connect(**)
SETTINGS_MAP = {
    "dbname": "NAME",
    "user": "USER",
    "password": "PASSWORD",
    "host": "HOST",
    "port": "PORT",
}

log = logging.getLogger(__name__)


class Command(BaseCommand):
    help = "Runs the async task runner"

    def add_arguments(self, parser: CommandParser) -> None:
        parser.add_argument(
            "--rescan-period",
            type=int,
            default=60,
            help="Forced task queue rescan interval, in seconds",
        )

        parser.add_argument(
            "--job-runners", type=int, default=1, help="Number of job runners to have"
        )

    def handle(self, rescan_period: int, job_runners: int, *args, **kwargs):
        if job_runners < 1:
            raise CommandError("Must have at least one job runner")

        channel_name = self.get_channel_name()
        database_alias = self.get_database_alias()

        db_settings = settings.DATABASES[database_alias]

        run_event = Event()
        run_event.set()  # Set it to start, since we want the runners to pop on startup
        exit_event = Event()

        def perform_exit(sig, frame):
            log.info("Caught exit signal")
            exit_event.set()

        signal.signal(signal.SIGINT, perform_exit)

        engine = db_settings["ENGINE"]
        log.debug("Database engine is %s", engine)

        execute_async = False
        if engine in NOTIFY_ENGINES:
            execute_async = True

        if not execute_async and not rescan_period:
            raise CommandError(
                "Either async notifications or a rescan period must be enabled"
            )

        if execute_async:
            log.info("Async notifications will be enabled")
        else:
            log.info("Async notifications are not being executed")

        if rescan_period:
            log.info("Rescan period is %d", rescan_period)
        else:
            log.warning("Periodic rescan is not enabled")

        stop_commands: List[Callable] = []
        waiting_threads: List[Thread] = []

        if execute_async:
            conn_args = {}

            # Copy the arguments to psycopg2, but leave the psycopg2 defaults
            # if the Django setting isn't set
            for psycopg2_key, django_key in SETTINGS_MAP.items():
                if db_settings.get(django_key):
                    conn_args[psycopg2_key] = db_settings[django_key]

            notification_thread = NotificationThread(
                channel_name=channel_name, conn_args=conn_args, run_event=run_event
            )

            notification_thread.start()

            # Wait in an external thread to fire exit
            # if the underlying notification thread exits
            wait_thread = WaitEvent(notification_thread, exit_event)
            wait_thread.start()

            # Register that this is a thread we want to wait on
            waiting_threads.append(notification_thread)

            # Register this thread as one we want to stop
            stop_commands.append(notification_thread.stop)

        if rescan_period:
            wakeup_thread = Wakeup(run_event, timedelta(seconds=rescan_period))
            wakeup_thread.start()

            # Make sure that, if the wait thread ever crashes (how?
            # we throw the exit event
            WaitEvent(wakeup_thread, exit_event).start()

        # Start the runners
        for i in range(job_runners):
            runner = JobRunner(run_event)
            runner.start()

            stop_commands.append(runner.stop)

            # Nobody should crash. If they do, begin the exit process
            WaitEvent(runner, exit_event).start()

        # Now sleep until something triggers an exit
        log.info("Waiting for exit event")
        exit_event.wait()
        log.info("Beginning exit routine")

        # Fire all the stop methods
        for func in stop_commands:
            log.debug("firing stop command: %s", func)
            func()

        log.debug("all exit functions fired")
        # Wait for all background threads to exit
        for thread in waiting_threads:
            thread.join()

        log.info("Exiting async runner")

    def get_channel_name(self):
        try:
            return settings.DBQUEUE_CHANNEL_NAME
        except AttributeError:
            return "dbqueue_notifications"

    def get_database_alias(self):
        try:
            return settings.DBQUEUE_DATABASE_ALIAS
        except AttributeError:
            return "default"

    def settings_check(self, settings_dict: dict) -> bool:
        if settings_dict["ENGINE"] not in ("django.db.backends.postgresql_psycopg2",):
            return False
