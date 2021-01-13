""" Models for database queue """

from __future__ import annotations


from datetime import timedelta
import functools
import importlib
import traceback

import logging

from typing import Callable, List, Dict, Any

from django.db import models, transaction
from django.utils import timezone


log = logging.getLogger(__name__)


class Uncallable(Exception):
    """ The underlying function was not callable """

    def __init__(self, arg):
        super().__init__()
        self.arg = arg

    def __str__(self):
        return f"Object of type '{self.arg.__name__}'' is not callable"


class PermanentFailure(Exception):
    """ The job will never be completed """

    def __init__(self, msg: str):
        super().__init__()

        self.msg = msg

    def __str__(self):
        return self.msg


class Unfinished(Exception):
    """ The job has not yet finished """


class JobManager(models.Manager):
    """ Manager for the job model """

    def simple_enqueue_job(self, func, *args, **kwargs):
        """ Enqueue a job for immediate execution with all default values """

        # Make sure the function is callable
        Job._callable_or_error(func)
        func_path = Job.function_to_path(func)

        with transaction.atomic():
            job = Job()
            job.func_name = func_path
            job.save()

            for i, arg in enumerate(args):
                job_arg = JobArg()
                job_arg.job = job
                job_arg.position = i
                job_arg.arg = arg

                job_arg.save()

            for key, value in kwargs.items():
                job_kwarg = JobKWArg()
                job_kwarg.job = job
                job_kwarg.param_name = key
                job_kwarg.arg = value

                job_kwarg.save()

        return job


class Job(models.Model):
    """ A job that needs to be run """

    # Full path to the function to call: a.b.c
    func_name = models.CharField(max_length=4096)

    # Time the job was queued up
    queued_at = models.DateTimeField(auto_now_add=True)

    # Priority to run the job at - lower is higher
    priority = models.PositiveSmallIntegerField(default=1000)

    # Run the job in the future
    delay_until = models.DateTimeField(null=True)

    # Delays due to errors
    error_delay_until = models.DateTimeField(null=True)

    # How many times to retry the job
    max_retries = models.PositiveSmallIntegerField(default=0)

    # Base time to delay retries for
    base_retry_delay = models.DurationField(default=timedelta(seconds=1))

    # What factor to scale the retries for after failure
    # For example, if the base_retry_delay is 30s and the scale factor is 2,
    # we will wait 30s, 1m, 2m, 4m, 8m between failures
    retry_multiplier = models.PositiveIntegerField(default=2)

    finished = models.BooleanField(default=False)

    objects = JobManager()

    def get_callable(self) -> Callable:
        """ Get a reference to the underlying callable """

        return Job.path_to_function(self.func_name)

    def get_partial(self):
        """ Get a function to execute with args and kwargs baked in """

        return functools.partial(
            self.get_callable(),
            *self.get_args(),
            **self.get_kwargs(),
        )

    def get_args(self) -> List[Any]:
        """ Get the args of the function, suitable for *args """

        out = []

        for obj in self.args.all().order_by("position"):
            assert isinstance(obj, JobArg)

            out.append(obj.arg)

        return out

    def get_kwargs(self) -> Dict[str, Any]:
        """ Get the kwargs of the function, suitable for **kwargs """

        out = {}

        for obj in self.kwargs.all():
            assert isinstance(obj, JobKWArg)

            out[obj.param_name] = obj.arg

        return out

    def execute(self):
        """ Execute the job and record the status """

        result = JobResult()
        result.job = self
        result.started_at = timezone.now()

        try:
            partial = self.get_partial()
        except Uncallable as exc:
            log.error("Unable to get partial callable for %s", self.func_name)

            # If this thing can't be called, never retry
            result.finished_at = timezone.now()

            result.permanent = True
            result.success = False
            result.exception = str(exc)

            self.finished = True
            self.save()

            # We aren't going to do a traceback here,
            # because it would just lead to like 7 lines up
            # The uncallable is a bit special cased in that regard

            result.save()
            return

        # Attempt execution
        try:
            val = partial()

            result.finished_at = timezone.now()
            result.success = True
            result.permanent = True
            result.result = val
            result.save()

            self.finished = True
            self.save()

        except Exception as exc:
            # Delay my own re-execution until the appropriate time
            total_try_count = self.results.count()

            # Set the internal delay for easy querying
            self.error_delay_until = (
                timezone.now()
                + self.base_retry_delay * self.retry_multiplier * total_try_count
            )

            result.finished_at = timezone.now()
            result.success = False
            result.permanent = False
            result.exception = str(exc)
            result.traceback = "\n".join(traceback.format_tb(exc.__traceback__))

            # If we've run out of retries, override permanent to true
            if total_try_count >= self.max_retries:
                result.permanent = True

            result.save()

            self.save()

    def get_result(self):
        """ Get the resultant value if available """

        try:
            permanent_result = self.results.get(permanent=True)
        except JobResult.DoesNotExist:
            # If we don't have a permanent result, throw back an unfinished
            raise Unfinished()

        assert isinstance(permanent_result, JobResult)

        if not permanent_result.success:
            raise PermanentFailure(permanent_result.exception)

        return permanent_result.result

    @staticmethod
    def function_to_path(func: Callable) -> str:
        """ Convert a function into a path for serialization """

        Job._callable_or_error(func)

        return f"{func.__module__}.{func.__name__}"

    @staticmethod
    def path_to_function(path: str) -> Callable:
        """ Convert a function path into the reference function """

        # Example: a.b.c means the a.b module, c function of that module
        parts = path.split(".")
        module_name = ".".join(parts[0:-1])
        func_name = parts[-1]

        module = importlib.import_module(module_name)
        func = getattr(module, func_name)

        Job._callable_or_error(func)

        return func

    @staticmethod
    def _callable_or_error(f: Any) -> None:
        if not callable(f):
            raise Uncallable(f)

    def __str__(self):
        return self.func


class JobArg(models.Model):
    """ A single argument for a job that needs to be run """

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="args")
    position = models.PositiveSmallIntegerField()
    arg = models.JSONField()

    class Meta:
        unique_together = (("job", "position"),)

    def __str__(self):
        return str(self.position)


class JobKWArg(models.Model):
    """ A keyword argument to a job """

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="kwargs")
    param_name = models.CharField(max_length=255)
    arg = models.JSONField()

    def __str__(self):
        return self.param_name


class JobResult(models.Model):
    """ The result of a run job """

    job = models.ForeignKey(Job, on_delete=models.CASCADE, related_name="results")
    success = models.BooleanField()
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField()
    exception = models.TextField()
    traceback = models.TextField()
    result = models.JSONField(null=True)
    permanent = models.BooleanField()  # Indicates if we should keep trying or not

    def __str__(self):
        return f"{self.job}: {self.success and 'Success' or 'Failure'}"
