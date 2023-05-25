"""validation test of DeequProfilerCommandlineArgumentsValidator module"""

# import: standard
from datetime import datetime

# import: datax in-house
from datax.utils.deployment_helper.validation.super_layer_endpoint import (
    BackdateLoadingCommandlineArgumentValidator,
)

# import: external
import pytest
from pydantic import ValidationError


def test_BackdateLoadingCommandlineArgumentValidator_without_date() -> None:
    """Test the `BackdateLoadingCommandlineArgumentValidator` class.

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

    arguments = BackdateLoadingCommandlineArgumentValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.start_date == test_dict["start_date"]
    assert arguments.end_date == test_dict["end_date"]


def test_BackdateLoadingCommandlineArgumentValidator_with_date() -> None:
    """Test the `BackdateLoadingCommandlineArgumentValidator` class.

    Assertion statement:
        1. Validate that the `module` argument is correctly validated.
        2. Validate that the 'start_date' and 'end_date' are validated and converted to datetime

    """
    test_dict = {
        "module": "test_module",
        "start_date": "2023-01-01",
        "end_date": "2023-01-02",
    }

    arguments = BackdateLoadingCommandlineArgumentValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.start_date == datetime.strptime(test_dict["start_date"], "%Y-%m-%d")
    assert arguments.end_date == datetime.strptime(test_dict["end_date"], "%Y-%m-%d")


def test_BackdateLoadingCommandlineArgumentValidator_with_date_wrong_format() -> None:
    """Test the `BackdateLoadingCommandlineArgumentValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when 'start_date' or 'end_date' is input in an incorrect format.

    """
    test_dict = {
        "module": "test_module",
        "start_date": "2023-01",
        "end_date": "2023-02",
    }
    with pytest.raises(ValidationError):
        BackdateLoadingCommandlineArgumentValidator(**test_dict)
