"""profiler-endpoint validation module"""

# import: standard
from typing import Optional

# import: datax in-house
from datax.utils.deployment_helper.validation.common import check_date_format
from datax.utils.deployment_helper.validation.common import (
    check_start_date_is_before_end_date,
)

# import: external
from pydantic import BaseModel
from pydantic import Extra
from pydantic import validator
from pydantic.class_validators import root_validator


class BackdateLoadingCommandlineArgumentValidator(BaseModel, extra=Extra.allow):
    """Pydantic class for validating BackdateLoading pipeline's commandline arguments.

    BackdateLoading pipeline will not require start_date and end_date and will only validate if there are inputs.

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
