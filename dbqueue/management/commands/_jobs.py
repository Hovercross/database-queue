""" Module for running the various jobs """

from datetime import timezone
from threading import Thread, Event
from typing import Union
import logging

from django.db import transaction
from django.db.models.query_utils import Q

from dbqueue import models

log = logging.getLogger(__name__)


class JobRunner(Thread):
    def __init__(self, run_event: Event):
        self.run_event = run_event
        self.exiting = False

        super().__init__(name="Async job runner")

    def run(self):
        # See if we can get any jobs

        while not self.exiting:
            log.debug("waiting on run event")
            self.run_event.wait()
            log.debug("got run event")

            while not self.exiting:
                # Run until we don't have a job to run
                with transaction.atomic():
                    job = self._get_job()

                    if job:
                        log.info("executing job %d", job.id)
                        job.execute()
                    else:
                        # Pull out of the while loop,
                        # since there are no more jobs to run
                        break

            if not self.exiting:
                # Now that we've run out of jobs to run,
                # clear the event so everyone stops
                self.run_event.clear()

    def _get_job(self) -> Union[models.Job, None]:
        # Jobs that are eligible to run
        time_query = Q(delay_until=None) | Q(delay_until__lte=timezone.now())

        error_time_query = Q(error_delay_until=None) | Q(
            error_delay_until__lte=timezone.now()
        )

        # Items without a permant result
        unfinished = Q(results__permanent=False) | Q(results__isnull=True)

        available_jobs = time_query & error_time_query & unfinished

        return (
            models.Job.objects.filter(available_jobs)
            .order_by("priority", "delay_until", "error_delay_until")
            .select_for_update(skip_locked=True)
            .first()
        )
