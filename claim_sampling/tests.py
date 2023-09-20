import warnings

from django.test import TestCase
from claim_sampling.services import example_service_function_job, ExampleService


class ExampleImisTest(TestCase):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        # Remove this code when implementing tests
        warnings.warn("The example code in test case is still present.")

    def test_example_module_loaded_correctly(self):
        example_service_function_job()
        ExampleService().example_service_method_job()
        self.assertTrue(True)
