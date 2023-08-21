"""validation test of DeequProfilerCommandlineArgumentsValidator module"""

# import: standard
import datetime

# import: datax in-house
from datax.utils.deployment_helper.validation.job_launcher_endpoint import (
    DateRangeWrapperCommandlineArgumentsValidator,
)

# import: external
import pytest
from pydantic import ValidationError


def test_DateRangeWrapperCommandlineArgumentsValidator() -> None:
    """
    Test the `DateRangeWrapperCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated.

    Assertion statement:
        1. Validate `module` arguments are correctly validated.
        2. Validate `start_date` arguments are correctly validated.
        3. Validate `end_date` arguments are correctly validated.
        4. Validate `job_id` arguments are correctly validated.
        4. Validate `task_type` arguments are correctly validated.
    """
    test_dict = {
        "module": "test_module",
        "start_date": "2023-05-06",
        "end_date": "2023-05-07",
        "job_id": 1234,
        "task_type": "notebook_task",
    }

    arguments = DateRangeWrapperCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.start_date == datetime.date.fromisoformat(test_dict["start_date"])
    assert arguments.end_date == datetime.date.fromisoformat(test_dict["end_date"])
    assert arguments.job_id == test_dict["job_id"]
    assert arguments.task_type == test_dict["task_type"]


def test_DateRangeWrapperCommandlineArgumentsValidator_wrong_job_id() -> None:
    """
    Test the `DateRangeWrapperCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when a wrong job_id is passed.
    """
    test_wrong_job_id_dict = {
        "module": "test_module",
        "start_date": "2023-05-06",
        "end_date": "2023-05-07",
        "job_id": "1234B",
    }
    with pytest.raises(ValidationError):
        DateRangeWrapperCommandlineArgumentsValidator(**test_wrong_job_id_dict)


def test_DateRangeWrapperCommandlineArgumentsValidator_wrong_date_format() -> None:
    """
    Test the `DateRangeWrapperCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValueError` is raised when a wrong date format is passed.
    """
    test_wrong_date_format = {
        "module": "test_module",
        "start_date": "20230506",
        "end_date": "20230507",
        "job_id": 1234,
    }

    with pytest.raises(ValueError):
        DateRangeWrapperCommandlineArgumentsValidator(**test_wrong_date_format)


def test_DateRangeWrapperCommandlineArgumentsValidator_wrong_date_config() -> None:
    """
    Test the `DateRangeWrapperCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when a wrong date config is passed.
    """
    test_wrong_date_config = {
        "module": "test_module",
        "start_date": "2023-05-08",
        "end_date": "2023-05-07",
        "job_id": 1234,
    }
    with pytest.raises(ValidationError):
        DateRangeWrapperCommandlineArgumentsValidator(**test_wrong_date_config)
