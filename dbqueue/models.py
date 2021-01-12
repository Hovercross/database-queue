""" Models for database queue """

from datetime import timedelta

import functools
import importlib

from typing import Callable, List, Dict, Any

from django.db import models


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

    # How many times to retry the job
    max_retries = models.PositiveSmallIntegerField(default=0)

    # Base time to delay retries for
    base_retry_delay = models.DurationField(default=timedelta(seconds=1))

    # What factor to scale the retries for after failure
    # For example, if the base_retry_delay is 30s and the scale factor is 2,
    # we will wait 30s, 1m, 2m, 4m, 8m between failures
    retry_multiplier = models.PositiveIntegerField(default=2)

    def get_callable(self) -> Callable:
        """ Get a reference to the underlying callable """

        # Example: a.b.c means the a.b module, c function of that module
        parts = self.func_name.split(".")
        module_name = parts.join(".")

        module = importlib.import_module(module_name)
        func = getattr(module, self.func_name)

        Job._callable_or_error(func)

        return func

    def get_partial(self):
        """ Get a function to execute with args and kwargs baked in """

        return functools.partial(
            self.get_callable(),
        )

    def get_args(self) -> List[Any]:
        """ Get the args of the function, suitable for *args """

        out = []

        for obj in self.args.all().order_by(position):
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
        module_name = ".".join(parts)

        module = importlib.import_module(module_name)
        func = getattr(module, self.func_name)

        Job._callable_or_error(func)

        return func

    @staticmethod
    def _callable_or_error(f: Any) -> None:
        if not callable(f):
            raise TypeError(f"'{func.__name__}' object is not callable")

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

    job = models.ForeignKey(Job, on_delete=models.CASCADE)
    success = models.BooleanField()
    started_at = models.DateTimeField()
    finished_at = models.DateTimeField()
    exception = models.TextField()
    traceback = models.TextField()
    result = models.JSONField(null=True)

    def __str__(self):
        return f"{self.job}: {self.success and 'Success' or 'Failure'}"
