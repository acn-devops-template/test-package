# import: standard
import os
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict

# import: external
from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator
from pydantic.class_validators import root_validator


def check_path_endswith_dot_json(v: str) -> str:
    _, file_extension = os.path.splitext(v)
    if file_extension != ".json":
        raise ValueError(
            f"Incorrect file format for the path `{v}`, the file should be `.json`"
        )
    return v


def check_start_date_is_before_end_date(cls: Callable, values: Dict) -> Dict:
    start_date, end_date = values.get("start_date"), values.get("end_date")
    if all([start_date is not None, end_date is not None, start_date > end_date]):
        raise ValueError(
            f"{start_date} > {end_date}. `start_date` must be before `end_date`"
        )
    return values


def check_date_format(cls: Callable, v: str) -> datetime:
    try:
        parsed_date = datetime.strptime(v, "%Y-%m-%d")
        return parsed_date
    except ValueError:
        raise ValueError(
            f"Incorrect date format for `{v}`, the date should have format of YYYY-MM-DD"
        )


class CommandlineArgumentValidator(BaseModel):
    module: str
    start_date: str
    end_date: str

    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )


class PipelineConfigArgumentValidators(BaseModel, extra=Extra.allow):
    data_processor_name: str
    main_transformation_name: str
    output_data_path: str
    output_schema_path: str

    _check_path_endswith_dot_json = validator("output_schema_path", allow_reuse=True)(
        check_path_endswith_dot_json
    )


class TransformationConfigArgumentValidator(BaseModel, extra=Extra.allow):
    input_schema_path: str
    ref_schema_path: str

    _check_path_endswith_dot_json = validator(
        "input_schema_path", "ref_schema_path", allow_reuse=True
    )(check_path_endswith_dot_json)
