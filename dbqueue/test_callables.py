""" Tests for the callable proxies """

from django.test import TestCase

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

    def test_enqueue_callable(self):
        self.job.execute()  # This will also save the results

        result = self.job.result()

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