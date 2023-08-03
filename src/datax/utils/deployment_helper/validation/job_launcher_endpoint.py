"""profiler-endpoint validation module"""

# import: datax in-house
from datax.utils.deployment_helper.validation.common import check_date_format
from datax.utils.deployment_helper.validation.common import (
    check_start_date_is_before_end_date,
)

# import: external
from pydantic import BaseModel
from pydantic import validator
from pydantic.class_validators import root_validator


class DatabricksJobCommandlineArgumentsValidator(BaseModel):
    """Pydantic class for validating profiler commandline arguments.

    For checking deequ profiler commandline arguments.

    Args:
        BaseModel: pydantic BaseModel.

    """

    module: str
    start_date: str
    end_date: str
    job_id: int
    task_type: str
    extra_params: dict
    
    if job_id < 0:
        raise ValueError("job_id must be positive number.")

    _check_date_format = validator("start_date", "end_date", allow_reuse=True)(
        check_date_format
    )
    _check_start_date_is_before_end_date = root_validator(allow_reuse=True)(
        check_start_date_is_before_end_date
    )
