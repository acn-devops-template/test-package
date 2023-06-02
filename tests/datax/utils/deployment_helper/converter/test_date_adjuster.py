""" date adjuster tests """

# import: standard
import unittest
from datetime import datetime
from unittest import mock

# import: datax in-house
from datax.utils.deployment_helper.converter.date_adjuster import (
    _convert_offset_into_dict,
)
from datax.utils.deployment_helper.converter.date_adjuster import check_input_date_format
from datax.utils.deployment_helper.converter.date_adjuster import compute_run_date
from datax.utils.deployment_helper.converter.date_adjuster import get_module_conf

# import: external
import pytest


class TestCheckInputDateFormat(unittest.TestCase):
    """Test Class for testing check_input_date_format.

    Class for testing check_input_date_format.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_check_input_date_format(self) -> None:
        """Test check_input_date_format."""

        input_date = "2022-02-02"
        output_date = check_input_date_format(input_date)

        self.assertEqual(output_date, "2022-02-02")
        self.assertEqual(type(output_date), str)

    def test_check_input_date_format_to_datetime(self) -> None:
        """Test check_input_date_format with to_datetime is True."""

        input_date = "2022-02-02"
        output_date = check_input_date_format(input_date, to_datetime=True)

        self.assertEqual(output_date, datetime(2022, 2, 2))
        self.assertEqual(type(output_date), datetime)

    def test_ValueError(self) -> None:
        """Test check_input_date_format.

        To test ValueError if found invalid input date

        """
        with pytest.raises(ValueError):
            check_input_date_format("Hello")


class TestGetModuleConf(unittest.TestCase):
    """Test Class for testing get_module_conf.

    Class for testing get_module_conf.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_get_module_conf(self) -> None:
        """Test function for testing get_module_conf for computing pipeline run date"""

        conf_path = "tests/resources/test_date_adjuster"
        pipeline = "TestABCModuleA"

        self.assertEqual(
            get_module_conf(conf_path, pipeline),
            {
                "TestABCModuleA": {
                    "data_processor_name": "datax",
                    "main_transformation_name": "tmp_credit_card",
                    "database": "default",
                    "table": "testing_pipeline",
                    "output_data_path": "spark-warehouse/resources/people",
                    "output_schema_path": "outputs/output_schema.json",
                    "date_offset": "days=-5",
                }
            },
        )


class TestComputeRunDate(unittest.TestCase):
    """Test Class for testing compute_run_date.

    Class for testing compute_run_date.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    @mock.patch(
        "datax.utils.deployment_helper.converter.date_adjuster.find_conf_path",
        return_value="tests/resources/test_date_adjuster",
    )
    def test_compute_run_date(self, mock_conf_path) -> None:
        """Test function for testing compute_run_date."""

        self.assertEqual(
            compute_run_date("2022-05-09", "TestABCModuleA"), "2022-05-04"
        )  # date_offset days=-5
        self.assertEqual(
            compute_run_date("2022-05-09", "TestABCModuleB"), "2022-05-06"
        )  # date_offset days=-3, no pipeline section
        self.assertEqual(
            compute_run_date("2022-05-09", "TestABCModuleCNoOffset"), "2022-05-07"
        )  # no date_offset, use default days=-2


class TestConvertOffsetIntoDict(unittest.TestCase):
    """Test Class for testing _convert_offset_into_dict.

    Class for testing _convert_offset_into_dict.

    Args:
        unittest.TestCase: An unittest TestCase.

    """

    def test_convert_offset_into_dict(self):
        """Test function for testing _convert_offset_into_dict."""

        test_input = "months = -4, days = 2"
        expected_output = {"months": -4, "days": 2}

        self.assertEqual(_convert_offset_into_dict(test_input), expected_output)
