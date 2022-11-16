"""abstract_class test of common modules"""

# import: standard
import pathlib
import unittest

# import: pyspark
from pyspark.sql import SparkSession

# import: datax in-house
from datax.utils.deployment_helper.abstract_class.common import Task

# import: external
import yaml


class Test_ABC_Common(unittest.TestCase):
    """
    Class for testing Task(ABC)
    """

    @unittest.mock.patch.multiple(Task, __abstractmethods__=set())
    def test(self):
        """
        main test function of Test_ABC_Common
        """
        self.instance = Task(module_name="Test_ABC_Module", conf_dir="./tests/resources/")

        firstValue = yaml.safe_load(
            pathlib.Path(
                "./tests/resources/test_pipeline/test_conf_file/Test_ABC_Module.yml"
            ).read_text()
        )
        message = "First value and second value are not equal !"

        secondValue = self.instance.conf
        # assertEqual() to check equality of first & second value
        self.assertEqual(firstValue, secondValue, message)
        self.assertIsInstance(self.instance.spark, SparkSession)
        self.assertRaises(
            ValueError,
            Task,
            module_name="Test_ABC_Module_2",
            conf_dir="./tests/resources/",
        )
        self.assertRaises(
            FileNotFoundError,
            Task,
            module_name="Test_ABC_Module_3",
            conf_dir="./tests/resources/",
        )
