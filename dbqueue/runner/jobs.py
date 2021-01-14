""" Module for running the various jobs """

from threading import Thread, Event
from typing import Union
import logging

from django.db import transaction, connections
from django.db.models.query_utils import Q
from django.utils import timezone

from dbqueue import models

log = logging.getLogger(__name__)


class JobRunner(Thread):
    def __init__(self, run_event: Event, name="Async job runner"):
        self.run_event = run_event
        self.exiting = False
        self.idle = Event()

        super().__init__(name=name)

    def run(self):
        # See if we can get any jobs

        while not self.exiting:
            log.debug("waiting on run event")
            self.run_event.wait()
            log.debug("got run event")

            self.idle.clear()

            while not self.exiting:
                # Mirroring the Django database handling set up in django.db
                for conn in connections.all():
                    conn.queries_log.clear()
                    conn.close_if_unusable_or_obsolete()

                # Run until we don't have a job to run
                with transaction.atomic():
                    job = self._get_job()

                    if job:
                        log.info("executing job %d", job.id)
                        job.execute()
                    else:
                        log.debug("breaking inner run loop")

                        # Pull out of the while loop,
                        # since there are no more jobs to run
                        break

            if not self.exiting:
                # Now that we've run out of jobs to run,
                # clear the event so everyone stops
                self.run_event.clear()

                # Mirror django.db - this is normally done in a singal handler
                # at the end of every job
                for conn in connections.all():
                    conn.queries_log.clear()
                    conn.close_if_unusable_or_obsolete()

            # This is mainly for testing, so I can catch these
            self.idle.set()

        log.debug("exiting run loop")

        # Grab all my connections and close them.
        # This may interfere with the Django connection handling, but this should
        # be run from a management command, and not from the request runner itself
        for conn in connections.all():
            conn.close()

    def stop(self):
        """ Flag ourself for stop """

        self.exiting = True
        # Flag the event so that we don't get stuck
        self.run_event.set()

    def _get_job(self) -> Union[models.Job, None]:
        log.debug("finding jobs")

        base = Q(final_result__isnull=True) & Q(canceled=False)

        # Jobs that are eligible to run
        time_query = Q(delay_until__isnull=True) | Q(delay_until__lte=timezone.now())

        error_time_query = Q(error_delay_until__isnull=True) | Q(
            error_delay_until__lte=timezone.now()
        )

        available_jobs = base & time_query & error_time_query

        return (
            models.Job.objects.filter(available_jobs)
            .order_by("priority", "delay_until", "error_delay_until")
            .select_for_update(skip_locked=True)
            .first()
        )
