"""validation test of CommandlineArgumentValidator, PipelineConfigArgumentValidators, TransformationConfigArgumentValidator modules"""

# import: standard
import unittest
from datetime import datetime

# import: datax in-house
from datax.utils.deployment_helper.validation.common import CommandlineArgumentValidator
from datax.utils.deployment_helper.validation.common import (
    OptionalDateCommandlineArgumentValidator,
)
from datax.utils.deployment_helper.validation.common import (
    PipelineConfigArgumentValidators,
)
from datax.utils.deployment_helper.validation.common import (
    TransformationConfigArgumentValidator,
)
from datax.utils.deployment_helper.validation.common import check_semantic_version_format

# import: external
import pytest
from pydantic import ValidationError


class Test_CommandlineArgumentValidator(unittest.TestCase):
    """Test Class for testing CommandlineArgumentValidator.

    Class for testing CommandlineArgumentValidator.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test(self) -> None:
        """Test function for Test_CommandlineArgumentValidator.

        Main test function of Test_CommandlineArgumentValidator.

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
            arguments.start_date,
            datetime.strptime(test_dict["start_date"], "%Y-%m-%d").date(),
        )
        self.assertEqual(
            arguments.end_date,
            datetime.strptime(test_dict["end_date"], "%Y-%m-%d").date(),
        )
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_1)
        self.assertRaises(ValueError, CommandlineArgumentValidator, **test_wrong_dict_2)
        self.assertRaises(
            ValidationError, CommandlineArgumentValidator, **test_wrong_dict_3
        )


class Test_PipelineConfigArgumentValidators(unittest.TestCase):
    """Test Class for testing PipelineConfigArgumentValidators.

    Class for testing PipelineConfigArgumentValidators.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test(self) -> None:
        """Test function for Test_PipelineConfigArgumentValidators.

        Main test function of Test_PipelineConfigArgumentValidators.

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
        self.assertRaises(
            ValueError, PipelineConfigArgumentValidators, **test_wrong_dict_1
        )
        self.assertRaises(
            ValidationError, PipelineConfigArgumentValidators, **test_wrong_dict_2
        )


class Test_TransformationConfigArgumentValidator(unittest.TestCase):
    """Test Class for testing TransformationConfigArgumentValidator.

    Class for testing TransformationConfigArgumentValidator.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test(self) -> None:
        """Test function for Test_TransformationConfigArgumentValidator.

        Main test function of Test_TransformationConfigArgumentValidator.

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
        self.assertRaises(
            ValueError, TransformationConfigArgumentValidator, **test_wrong_dict_1
        )
        self.assertRaises(
            ValidationError, TransformationConfigArgumentValidator, **test_wrong_dict_2
        )


def test_check_semantic_version_format() -> None:
    """Test function to check if version format is in the format X.Y.Z

    Assertion statement:
        1. To check if version format is correct
    """
    valid_versions = ["1.0.0", "0.1.0", "10.20.30", "0.1.0-rc1"]

    for version in valid_versions:
        assert (
            check_semantic_version_format(None, version) == version
        ), "Invalid release version format (X.Y.Z)"


def test_check_semantic_version_format_invalid() -> None:
    """Test function to check if version format is in the format X.Y.Z

    Assertion statement:
        1. To check if version format is correct
    """
    invalid_versions = [
        "version1",
        "x.y.z",
        "1_2_3",
        "1.0.0-rc.1",
        "1.0.0-hotfix.1",
        "1.0.0-hotfix1",
    ]

    for version in invalid_versions:
        with pytest.raises(ValueError):
            check_semantic_version_format(
                None, version
            ), "Method does not raise error when incorrect format"


def test_OptionalDateCommandlineArgumentValidator_without_date() -> None:
    """Test the `OptionalDateCommandlineArgumentValidator` class.

    To validate if not inputting 'start_date' and 'end_date' is allowed.

    Assertion statement:
        1. Validate that the `module` argument is correctly validated.
        2. Validate that the 'start_date' and 'end_date' pass the validation and return 'None'

    """
    test_dict = {
        "module": "test_module",
        "start_date": None,
        "end_date": None,
    }

    arguments = OptionalDateCommandlineArgumentValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.start_date == test_dict["start_date"]
    assert arguments.end_date == test_dict["end_date"]


def test_OptionalDateCommandlineArgumentValidator_with_date() -> None:
    """Test the `OptionalDateCommandlineArgumentValidator` class.

    Assertion statement:
        1. Validate that the `module` argument is correctly validated.
        2. Validate that the 'start_date' and 'end_date' are validated and converted to datetime

    """
    test_dict = {
        "module": "test_module",
        "start_date": "2023-01-01",
        "end_date": "2023-01-02",
    }

    arguments = OptionalDateCommandlineArgumentValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert (
        arguments.start_date
        == datetime.strptime(test_dict["start_date"], "%Y-%m-%d").date()
    )
    assert (
        arguments.end_date == datetime.strptime(test_dict["end_date"], "%Y-%m-%d").date()
    )


def test_OptionalDateCommandlineArgumentValidator_with_date_wrong_format() -> None:
    """Test the `OptionalDateCommandlineArgumentValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when 'start_date' or 'end_date' is input in an incorrect format.

    """
    test_dict = {
        "module": "test_module",
        "start_date": "2023-01",
        "end_date": "2023-02",
    }
    with pytest.raises(ValidationError):
        OptionalDateCommandlineArgumentValidator(**test_dict)
