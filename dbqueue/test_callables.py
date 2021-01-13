""" Tests for the callable proxies """

from threading import Event
from typing import List

from django.test import TestCase
from django.test.testcases import TransactionTestCase

from dbqueue.runner.jobs import JobRunner

from . import models


def test_func(*args, **kwargs):
    """ A simple test function that we can import externally """

    # Coerce the types, because occasionally I get a list vs a tuple
    return {"args": list(args), "kwargs": dict(kwargs)}


class TestGetCallable(TestCase):
    def test_get_get_callable_name(self):
        self.assertEqual(
            models.Job.function_to_path(test_func), "dbqueue.test_callables.test_func"
        )

    def test_get_callable(self):
        self.assertEqual(
            models.Job.path_to_function("dbqueue.test_callables.test_func"), test_func
        )

    def test_callable(self):
        f = models.Job.path_to_function("dbqueue.test_callables.test_func")
        result = f(1, a="b", c="d")

        self.assertEqual(
            result,
            {
                "args": [1],
                "kwargs": {
                    "a": "b",
                    "c": "d",
                },
            },
        )


class TestEnqueueCallable(TestCase):
    def setUp(self):
        self.job = models.Job.objects.simple_enqueue_job(test_func, 1, a="b", c="d")

    def test_enqueue_callable(self):
        job = models.Job.objects.get(pk=self.job.pk)
        assert isinstance(job, models.Job)

        self.assertEqual(job.get_callable(), test_func)

        arg_1 = job.args.all().get()
        assert isinstance(arg_1, models.JobArg)

        self.assertEqual(arg_1.position, 0)
        self.assertEqual(arg_1.arg, 1)

        kwarg_a = job.kwargs.get(param_name="a")
        assert isinstance(kwarg_a, models.JobKWArg)
        self.assertEqual(kwarg_a.arg, "b")

        kwarg_a = job.kwargs.get(param_name="a")
        assert isinstance(kwarg_a, models.JobKWArg)
        self.assertEqual(kwarg_a.arg, "b")


class TestSuccessfulExecution(TestCase):
    def setUp(self):
        self.job = models.Job.objects.simple_enqueue_job(test_func, 1, a="b", c="d")

    def test_execute(self):
        self.job.execute()  # This will also save the results

        result = self.job.get_result()

        self.assertEqual(
            result,
            {
                "args": [1],
                "kwargs": {
                    "a": "b",
                    "c": "d",
                },
            },
        )


class TestJobRunner(TransactionTestCase):
    def setUp(self):
        self.job = models.Job.objects.simple_enqueue_job(test_func, 1, a="b", c="d")

    def test_run(self):
        run_event = Event()
        runner = JobRunner(run_event)
        runner.start()
        run_event.set()

        # Wait for the runner's idle to flag, which means
        # that it has gone through at least one
        # loop and determined there are no more jobs

        runner.idle.wait()
        runner.stop()

        # Wait for the runner to finish
        runner.join()

        self.job.refresh_from_db()
        result = self.job.get_result()

        self.assertEqual(
            result,
            {
                "args": [1],
                "kwargs": {
                    "a": "b",
                    "c": "d",
                },
            },
        )


class TestMultipleJobRunnerMultipleJobs(TransactionTestCase):
    def setUp(self):
        self.jobs = []

        for i in range(1000):
            # 0, 1, 2... 999
            self.jobs.append(models.Job.objects.simple_enqueue_job(test_func, i))

    def test_run(self):
        run_event = Event()

        runners: List[JobRunner] = []
        for i in range(5):
            runner = JobRunner(run_event, name=f"Job runner {i}")
            runners.append(runner)
            runner.start()

        # Set everything off to the races
        run_event.set()

        # Wait for all the runners to go idle
        for runner in runners:
            runner.idle.wait()

        for runner in runners:
            runner.stop()

        for runner in runners:
            runner.join()

        for i, job in enumerate(self.jobs):
            assert isinstance(job, models.Job)

            # job 0 should have an arg of 0, 1 an arg of 1, etc

            job.refresh_from_db()

            result = job.get_result()
            self.assertEqual(result, {"args": [i], "kwargs": {}})
