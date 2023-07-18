"""validation test of ProfilerCommandlineArgumentsValidator module"""

# import: standard
from datetime import datetime
from pathlib import Path

# import: datax in-house
from datax.utils.deployment_helper.validation.profiler_endpoint import (
    ProfilerCommandlineArgumentsValidator,
)

# import: external
import pytest
from pydantic import ValidationError


def test_ProfilerCommandlineArgumentsValidator() -> None:
    """Test the `ProfilerCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted.

    Assertion statement:
        1. Validate `module`, `data_source` and `version` arguments are correctly validated.
        2. Validate that the module correctly sets the `is_adhoc` variable to False
            since `data_source` argument is provided.
    """
    test_dict = {
        "module": "test_module",
        "data_source": "MockCreditCardPipeline",
        "version": "0.0.1",
    }

    arguments = ProfilerCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.data_source == test_dict["data_source"]
    assert arguments.version == test_dict["version"]
    assert arguments.is_adhoc is False


def test_ProfilerCommandlineArgumentsValidator_adhoc_profiling() -> None:
    """Test the `ProfilerCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted for adhoc-profiling run.

    Assertion statement:
        1. Validate `conf_profile_path` is checked for existence and converted to a Path object.
        2. Validate `module`, `start_date` and `end_date` arguments are correctly validated
            and converted to the correct format.
        3. Validate that the module correctly sets the `is_adhoc` variable to True
            since `data_source` argument is not provided.
    """
    test_dict = {
        "module": "test_module",
        "conf_profile_path": "tests/resources/people.json",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    arguments = ProfilerCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert isinstance(arguments.conf_profile_path, Path)
    assert (
        arguments.start_date
        == datetime.strptime(test_dict["start_date"], "%Y-%m-%d").date()
    )
    assert (
        arguments.end_date == datetime.strptime(test_dict["end_date"], "%Y-%m-%d").date()
    )
    assert arguments.is_adhoc is True


def test_ProfilerCommandlineArgumentsValidator_wrong_conf_profile_path() -> None:
    """Test the `ProfilerCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when a non-existent profile path is passed.
    """
    test_dict = {
        "module": "test_module",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
        "conf_profile_path": "mock_dir/nonexistent_folder/app.yml",
    }

    with pytest.raises(ValidationError):
        ProfilerCommandlineArgumentsValidator(**test_dict)


def test_ProfilerCommandlineArgumentsValidator_check_profiling_without_source_inputs() -> (
    None
):
    """Test the `ProfilerCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when neither `data_source` nor
            `conf_profile_path` are provided.
    """
    input_dict = {
        "module": "test_module",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    with pytest.raises(ValidationError) as exc_info:
        ProfilerCommandlineArgumentsValidator(**input_dict)

    assert (
        exc_info.value.errors()[0]["msg"]
        == "Either data_source or conf_profile_path must be provided."
    )
