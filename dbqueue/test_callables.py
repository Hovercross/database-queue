""" Tests for the callable proxies """

from django.test import TestCase

from . import models


def test_func(*args, **kwargs):
    """ A simple test function that we can import externally """

    return {"args": args, "kwargs": kwargs}


class TestGetCallable(TestCase):
    def test_get_get_callable_name(self):
        self.assertEqual(
            models.Job.function_to_path(test_func), "dbqueue.test_callables.test_func"
        )

    def test_get_callable(self):
        self.assertEqual(
            models.Job.path_to_function("dbqueue.test_callables.test_func"), test_func
        )