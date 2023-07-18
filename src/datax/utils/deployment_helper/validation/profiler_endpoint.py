"""profiler-endpoint validation module"""

# import: standard
from typing import Callable
from typing import Optional

# import: datax in-house
from datax.utils.deployment_helper.validation.common import check_date_format
from datax.utils.deployment_helper.validation.common import check_semantic_version_format
from datax.utils.deployment_helper.validation.common import (
    check_start_date_is_before_end_date,
)

# import: external
from pydantic import BaseModel
from pydantic import Extra
from pydantic import FilePath
from pydantic import validator
from pydantic.class_validators import root_validator


def check_profiling_source(cls: Callable, values: dict) -> dict:
    """Function to check profiling source, data_source name or adhoc-profiling input conf_profile_path.

    If data_source (data pipeline module) is provided, set `is_adhoc` flag to False.
    Otherwise, `is_adhoc` flag is True.

    Args:
        cls (Callable): cls.
        values (dict): Dictionary containing validated values.

    Returns:
        dict: An input dict

    Raises:
        ValueError: If neither data_source nor conf_profile_path are provided.

    """
    data_source = values.get("data_source")
    conf_profile_path = values.get("conf_profile_path")

    if not data_source and not conf_profile_path:
        raise ValueError("Either data_source or conf_profile_path must be provided.")

    values["is_adhoc"] = False if data_source is not None else True

    return values


class ProfilerCommandlineArgumentsValidator(BaseModel, extra=Extra.allow):
    """Pydantic class for validating profiler commandline arguments.

    For checking profiler commandline arguments.

    Args:
        BaseModel: pydantic BaseModel.
        extra: pydantic Extra.allow

    """

    module: str
    start_date: Optional[str]
    end_date: Optional[str]
    data_source: Optional[str]
    conf_profile_path: Optional[FilePath]
    version: Optional[str]

    _check_profiling_source = root_validator(allow_reuse=True)(check_profiling_source)
    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )
    _check_version_format = validator("version", allow_reuse=True)(
        check_semantic_version_format
    )
