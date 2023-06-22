"""validation test of DriftEndpointCommandlineArgumentsValidator module"""

# import: standard
from datetime import datetime
from pathlib import Path

# import: datax in-house
from datax.utils.deployment_helper.validation.drift_endpoint import (
    DriftEndpointCommandlineArgumentsValidator,
)

# import: external
import pytest
from pydantic import ValidationError


def test_DriftEndpointCommandlineArgumentsValidator() -> None:
    """Test the `DriftEndpointCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted.

    Assertion statement:
        1. Validate `module`,`data_source`, `start_date`, `end_date` and `version` arguments are correctly validated.
        2. Validate that the module correctly sets the `is_adhoc` variable to False
            since `data_source` argument is provided.
    """
    test_dict = {
        "module": "test_module",
        "data_source": "MockMLopsPipeline",
        "version": "1.0.0",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    arguments = DriftEndpointCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.data_source == test_dict["data_source"]
    assert arguments.version == test_dict["version"]
    assert (
        arguments.start_date
        == datetime.strptime(test_dict["start_date"], "%Y-%m-%d").date()
    )
    assert (
        arguments.end_date == datetime.strptime(test_dict["end_date"], "%Y-%m-%d").date()
    )
    assert arguments.is_adhoc is False


def test_DriftEndpointCommandlineArgumentsValidator_adhoc_drift() -> None:
    """Test the `DriftEndpointCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted for adhoc-drift run.

    Assertion statement:
        1. Validate `module` arguments are correctly validated.
        2. Validate `conf_profile_path` is checked for existence and converted to a Path object.
        3. Validate `start_date` and `end_date` arguments are correctly validated
            and converted to the correct format.
        4. Validate that the module correctly sets the `is_adhoc` variable to True
            since `data_source` argument is not provided.
    """
    test_dict = {
        "module": "test_module",
        "version": "1.0.0",
        "conf_profile_path": "tests/resources/people.json",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    arguments = DriftEndpointCommandlineArgumentsValidator(**test_dict)
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


def test_DriftEndpointCommandlineArgumentsValidator_wrong_conf_profile_path() -> None:
    """Test the `DriftEndpointCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when a non-existent profile path is passed.
    """
    test_dict = {
        "module": "test_module",
        "version": "1.0.0",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
        "conf_profile_path": "mock_dir/nonexistent_folder/app.yml",
    }

    with pytest.raises(ValidationError):
        DriftEndpointCommandlineArgumentsValidator(**test_dict)


def test_DriftEndpointCommandlineArgumentsValidator_check_profiling_without_source_inputs() -> (
    None
):
    """Test the `DriftEndpointCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when neither `data_source` nor
            adhoc-profiling inputs are provided.
    """
    input_dict = {
        "module": "test_module",
        "version": "1.0.0",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    with pytest.raises(ValidationError) as exc_info:
        DriftEndpointCommandlineArgumentsValidator(**input_dict)

    assert (
        exc_info.value.errors()[0]["msg"]
        == "Either data_source or adhoc-profiling inputs: conf_profile_path must be provided."
    )


def test_DriftEndpointCommandlineArgumentsValidator_check_profiling_without_version_inputs() -> (
    None
):
    """Test the `DriftEndpointCommandlineArgumentsValidator` class.

    Assertion statement:
        1. Validate if a `ValidationError` is raised when neither `data_source` nor
            adhoc-profiling inputs are provided.
    """
    input_dict = {
        "module": "test_module",
        "data_source": "MockMLopsPipeline",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    with pytest.raises(ValidationError) as exc_info:
        DriftEndpointCommandlineArgumentsValidator(**input_dict)

    assert exc_info.value.errors()[0]["msg"] == "version must be provided."
