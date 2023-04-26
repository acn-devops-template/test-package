"""validation test of DeequProfilerCommandlineArgumentsValidator module"""

# import: standard
from datetime import datetime
from pathlib import Path

# import: datax in-house
from datax.utils.deployment_helper.validation.profiler_endpoint import (
    DeequProfilerCommandlineArgumentsValidator,
)

# import: external
import pytest
from pydantic import FilePath
from pydantic import ValidationError


def test_DeequProfilerCommandlineArgumentsValidator() -> None:
    """Test the `DeequProfilerCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted.

    Raises:
        AssertionError: If any of the test assertions fail.
    """
    test_dict = {
        "module": "test_module",
        "data_source": "MockCreditCardPipeline",
    }

    arguments = DeequProfilerCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.data_source == test_dict["data_source"]
    assert arguments.is_adhoc is False


def test_DeequProfilerCommandlineArgumentsValidator_adhoc_profiling() -> None:
    """Test the `DeequProfilerCommandlineArgumentsValidator` class.

    To validate the arguments are correctly validated and converted for adhoc-profiling run.

    Raises:
        AssertionError: If any of the test assertions fail.
    """
    test_dict = {
        "module": "test_module",
        "database": "test_database",
        "table": "test_table",
        "date_column": "dl_data_dt",
        "conf_profile_path": "tests/resources/people.json",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    arguments = DeequProfilerCommandlineArgumentsValidator(**test_dict)

    assert arguments.module == test_dict["module"]
    assert arguments.database == test_dict["database"]
    assert arguments.table == test_dict["table"]
    assert arguments.date_column == test_dict["date_column"]
    assert isinstance(arguments.conf_profile_path, Path)
    assert arguments.start_date == datetime.strptime(test_dict["start_date"], "%Y-%m-%d")
    assert arguments.end_date == datetime.strptime(test_dict["end_date"], "%Y-%m-%d")
    assert arguments.is_adhoc is True


def test_DeequProfilerCommandlineArgumentsValidator_wrong_conf_profile_path() -> None:
    """Test the `DeequProfilerCommandlineArgumentsValidator` class.

    To validate that a `ValidationError` is raised when a non-existent profile path is passed.

    Raises:
        AssertionError: If any of the test assertions fail.
    """
    test_dict = {
        "module": "test_module",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
        "database": "test_db",
        "table": "test_table",
        "date_column": "dl_data_dt",
        "conf_profile_path": "mock_dir/nonexistent_folder/app.yml",
    }

    with pytest.raises(ValidationError):
        DeequProfilerCommandlineArgumentsValidator(**test_dict)


def test_DeequProfilerCommandlineArgumentsValidator_check_profiling_without_source_inputs() -> (
    None
):
    """Test the `DeequProfilerCommandlineArgumentsValidator` class.

    To validate that a `ValidationError` is raised when neither `data_source` nor
    adhoc-profiling inputs are provided.

    Raises:
        AssertionError: If any of the test assertions fail.
    """
    input_dict = {
        "module": "test_module",
        "start_date": "2022-01-01",
        "end_date": "2022-01-02",
    }

    with pytest.raises(ValidationError) as exc_info:
        DeequProfilerCommandlineArgumentsValidator(**input_dict)

    assert (
        exc_info.value.errors()[0]["msg"]
        == "Either data_source or adhoc-profiling inputs: database, table, date_column and conf_profile_path must be provided."
    )
