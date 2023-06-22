"""drift-endpoint validation module"""

# import: standard
from typing import Callable
from typing import Optional

# import: datax in-house
from datax.utils.deployment_helper.validation.common import check_date_format
from datax.utils.deployment_helper.validation.common import (
    check_start_date_is_before_end_date,
)

# import: external
from pydantic import BaseModel
from pydantic import Extra
from pydantic import FilePath
from pydantic import validator
from pydantic.class_validators import root_validator


def check_drift_profiling_source(cls: Callable, values: dict) -> dict:
    """Function to check if data_source is provided or input for adhoc profiling
    including database, table and partition_col and conf_profile_path are provided.
    Args:
        values (dict): Dictionary containing validated values.
    Returns:
        dict: Dictionary containing validated values.
    Raises:
        ValueError: If neither data_source nor database, table, partition_col and
        conf_profile_path are provided.
    """
    data_source = values.get("data_source")
    version = values.get("version")
    conf_profile_path = values.get("conf_profile_path")
    if not (version):
        raise ValueError("version must be provided.")

    if not (data_source or (conf_profile_path)):
        raise ValueError(
            "Either data_source or adhoc-profiling inputs: conf_profile_path must be provided."
        )

    values["is_adhoc"] = False if data_source is not None else True

    return values


class DriftEndpointCommandlineArgumentsValidator(BaseModel, extra=Extra.allow):
    """Pydantic class for validating commandline arguments related to engine-endpoint.
    This model inherits from BaseCommandlineArgumentValidator for checking
    additional commandline arguments related to engine-endpoint.
    Args:
        BaseModel: pydantic BaseModel.
        extra: pydantic Extra.allow
    """

    module: str
    start_date: str
    end_date: str
    data_source: Optional[str]
    conf_profile_path: Optional[FilePath]

    _check_profiling_source = root_validator(allow_reuse=True)(
        check_drift_profiling_source
    )
    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )
