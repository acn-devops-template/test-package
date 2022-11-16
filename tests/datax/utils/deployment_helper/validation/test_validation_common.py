"""validation test of CommandlineArgumentValidator, PipelineConfigArgumentValidators, TransformationConfigArgumentValidator modules"""

# import: standard
import unittest
from datetime import datetime

# import: datax in-house
from datax.utils.deployment_helper.validation.common import CommandlineArgumentValidator
from datax.utils.deployment_helper.validation.common import (
    PipelineConfigArgumentValidators,
)
from datax.utils.deployment_helper.validation.common import (
    TransformationConfigArgumentValidator,
)

# import: external
from pydantic import ValidationError


class Test_CommandlineArgumentValidator(unittest.TestCase):
    """
    Class for testing CommandlineArgumentValidator
    """

    def test(self):
        """
        main test function of Test_CommandlineArgumentValidator
        """
        test_dict = {
            "module": "test_module",
            "start_date": "2022-01-27",
            "end_date": "2022-01-27",
        }
        test_wrong_dict_1 = {
            "module": "test_module",
            "start_date": "20220101",
            "end_date": "20220101",
        }
        test_wrong_dict_2 = {
            "module": "test_module",
            "start_date": "2022-01-05",
            "end_date": "2022-01-01",
        }
        test_wrong_dict_3 = {
            "module": "test_module",
            "start_date": "2022-01-05",
            "test1": "val1",
            "test2": "val2",
        }
        arguments = CommandlineArgumentValidator(**test_dict)

        self.assertEqual(arguments.module, test_dict["module"])
        self.assertEqual(
            arguments.start_date, datetime.strptime(test_dict["start_date"], "%Y-%m-%d")
        )
        self.assertEqual(
            arguments.end_date, datetime.strptime(test_dict["end_date"], "%Y-%m-%d")
        )
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_1)
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_2)
        self.assertRaises(
            ValidationError, CommandlineArgumentValidator, **test_wrong_dict_3
        )


class Test_PipelineConfigArgumentValidators(unittest.TestCase):
    """
    Class for testing PipelineConfigArgumentValidators
    """

    def test(self):
        """
        main test function of Test_PipelineConfigArgumentValidators
        """
        test_dict = {
            "data_processor_name": "test",
            "main_transformation_name": "test",
            "output_data_path": "test/test",
            "output_schema_path": "test.json",
        }
        test_wrong_dict_1 = {
            "data_processor_name": "test",
            "main_transformation_name": "test",
            "output_data_path": "test/test",
            "output_schema_path": "test",
        }
        test_wrong_dict_2 = {
            "data_processor_name": "test",
            "main_transformation_name": "test",
            "output_data_path": "test/test",
        }
        arguments = PipelineConfigArgumentValidators(**test_dict)

        self.assertEqual(arguments.data_processor_name, test_dict["data_processor_name"])
        self.assertEqual(
            arguments.main_transformation_name, test_dict["main_transformation_name"]
        )
        self.assertEqual(arguments.output_data_path, test_dict["output_data_path"])
        self.assertEqual(arguments.output_schema_path, test_dict["output_schema_path"])
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_1)
        self.assertRaises(
            ValidationError, CommandlineArgumentValidator, **test_wrong_dict_2
        )


class Test_TransformationConfigArgumentValidator(unittest.TestCase):
    """
    Class for testing TransformationConfigArgumentValidator
    """

    def test(self):
        """
        main test function of Test_TransformationConfigArgumentValidator
        """
        test_dict = {
            "input_data_endpoint": "test",
            "input_schema_path": "test.json",
            "ref_schema_path": "test.json",
        }
        test_wrong_dict_1 = {
            "input_data_endpoint": "test",
            "input_schema_path": "test",
            "ref_schema_path": "test.json",
        }
        test_wrong_dict_2 = {
            "input_data_endpoint": "test",
            "input_schema_path": "test.json",
        }
        arguments = TransformationConfigArgumentValidator(**test_dict)

        self.assertEqual(arguments.input_data_endpoint, test_dict["input_data_endpoint"])
        self.assertEqual(arguments.input_schema_path, test_dict["input_schema_path"])
        self.assertEqual(arguments.ref_schema_path, test_dict["ref_schema_path"])
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_1)
        self.assertRaises(
            ValidationError, CommandlineArgumentValidator, **test_wrong_dict_2
        )
