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
    retry_delay = models.DurationField(default=timedelta(seconds=1))

    final_result = models.ForeignKey(
        "JobResult", on_delete=models.DO_NOTHING, related_name="+", null=True
    )

    canceled = models.BooleanField(default=False)

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

        # Get the partial. If we can't get it, immediately fail out and stop retries
        try:
            partial = self.get_partial()
        except Uncallable as exc:
            log.error("Unable to get partial callable for %s", self.func_name)

            # If this thing can't be called, never retry,
            # and don't bother with a traceback - it'll just point to here and
            # not anything useful

            result.finished_at = timezone.now()
            result.success = False
            result.exception = str(exc)
            result.save()

            # This is the permanent result
            self.final_result = result
            self.save()

            return

        # We have a partial - attempt execution
        try:
            val = partial()

            # If we made it to here, we got success
            result.finished_at = timezone.now()
            result.success = True
            result.result = val
            result.save()

            self.final_result = result
            self.save()

        except Exception as exc:
            # No matter what, record the execution results
            result.finished_at = timezone.now()
            result.success = False
            result.exception = str(exc)
            result.traceback = "\n".join(traceback.format_tb(exc.__traceback__))
            result.save()

            attempt_count = self.results.count()

            # If we have already hit retries,
            # indicate this is the final result and bail
            if attempt_count > self.max_retries:
                self.final_result = result
                self.save()

                return

            # We haven't hit max retries yet - schedule the next one out
            delay = self.retry_delay ** attempt_count
            self.error_delay_until = timezone.now() + delay
            self.save()

    def get_result(self):
        """ Get the resultant value if available """

        if not self.final_result:
            raise Unfinished()

        assert isinstance(self.final_result, JobResult)

        if not self.final_result.success:
            raise PermanentFailure(self.final_result.exception)

        return self.final_result.result

    @property
    def should_retry(self):
        """ Determine if we can now execute """

        if self.final_result:
            return False

        if self.results.count() >= self.max_retries:
            return False

        return True

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

    def __str__(self):
        return f"{self.job}: {self.success and 'Success' or 'Failure'}"
