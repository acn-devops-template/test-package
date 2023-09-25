"""validation common module"""

# import: standard
import os
import re
from datetime import date
from datetime import datetime
from typing import Callable
from typing import Dict
from typing import Optional

# import: external
from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator
from pydantic.class_validators import root_validator


def check_path_endswith_dot_json(v: str) -> str:
    """Function to check inputs that end with '.json'.

    Check if the input str ends with '.json'.

    Args:
        v (str): An input str.

    Returns:
        str: An input str.

    Raises:
        ValueError: If v doea not end with '.json'.

    """
    _, file_extension = os.path.splitext(v)
    if file_extension != ".json":
        raise ValueError(
            f"Incorrect file format for the path `{v}`, the file should be `.json`"
        )
    return v


def check_start_date_is_before_end_date(cls: Callable, values: Dict) -> Dict:
    """Function to check if start_date is before end_date.

    Check if start_date is before end_date

    Args:
        cls (Callable): cls.
        values (Dict): An input dict.

    Returns:
        Dict: An input dict.

    Raises:
        ValueError: If start_date > end_date.

    """
    start_date, end_date = values.get("start_date"), values.get("end_date")
    if start_date is not None and end_date is not None and start_date > end_date:
        raise ValueError(
            f"{start_date} > {end_date}. `start_date` must be before `end_date`"
        )
    return values


def check_date_format(cls: Callable, v: str) -> date:
    """Function to check if start_date and end_date are in the correct format.

    Check if the input str is in a correct datetime format (YYYY-MM-DD)

    Args:
        cls (Callable): cls.
        v (str): An input str.

    Returns:
        date: An input str casted into date.

    Raises:
        ValueError: If incorrect date format (YYYY-MM-DD).

    """
    if v is not None:
        try:
            parsed_date = datetime.strptime(v, "%Y-%m-%d").date()
            return parsed_date
        except ValueError:
            raise ValueError(
                f"Incorrect date format for `{v}`, the date should have format of YYYY-MM-DD"
            )
    return v


def check_semantic_version_format(cls: Callable, v: str) -> str:
    """Function to check if version format respects Sematic Versioning (MAJOR.MINOR.PATCH)

    Args:
        cls (Callable): cls.
        v (str): An input str.

    Returns:
        str: An input str of version.

    Raises:
        ValueError: Invalid semantic version format (X.Y.Z)

    """
    pattern = r"^(\d+!)?(\d+)(\.\d+)+([\.\-\_])?((a(lpha)?|b(eta)?|c|r(c|ev)?|pre(view)?)\d*)?(\.?(post|dev)\d*)?$"
    if re.match(pattern, v):
        return v
    else:
        raise ValueError("Invalid semantic version format (X.Y.Z)")


class CommandlineArgumentValidator(BaseModel):
    """Pydantic class for validating commandline arguments.

    For checking commandline arguments.

    Args:
        BaseModel: pydantic BaseModel.

    """

    module: str
    start_date: str
    end_date: str

    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )


class PipelineConfigArgumentValidators(BaseModel, extra=Extra.allow):  # type: ignore
    """Pydantic class for validating pipeline arguments.

     For checking pipeline conf arguments

    Args:
        BaseModel: pydantic BaseModel.
        extra: pydantic Extra.allow

    """

    data_processor_name: str
    main_transformation_name: str
    output_data_path: str
    output_schema_path: str

    _check_path_endswith_dot_json = validator("output_schema_path", allow_reuse=True)(
        check_path_endswith_dot_json
    )


class TransformationConfigArgumentValidator(BaseModel, extra=Extra.allow):  # type: ignore
    """Pydantic class for validating transformation conf arguments.

     For checking transformation conf argument (data_source)

    Args:
        BaseModel: pydantic BaseModel.
        extra: pydantic Extra.allow

    """

    input_schema_path: str
    ref_schema_path: str

    _check_path_endswith_dot_json = validator(
        "input_schema_path", "ref_schema_path", allow_reuse=True
    )(check_path_endswith_dot_json)


class OptionalDateCommandlineArgumentValidator(BaseModel, extra=Extra.allow):
    """Pydantic class for validating pipeline with dates as optional input's commandline arguments.

    For example, streaming pipeline will not require start_date and end_date and will only validate if there are inputs.

    Args:
        BaseModel: pydantic BaseModel.
        extra: pydantic Extra.allow

    """

    module: str
    start_date: Optional[str]
    end_date: Optional[str]

    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )
