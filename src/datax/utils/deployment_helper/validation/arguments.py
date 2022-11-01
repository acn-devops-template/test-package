# import: standard
import os
from datetime import datetime
from typing import Any
from typing import Callable
from typing import Dict

# import: external
from pydantic import BaseModel
from pydantic import validator
from pydantic.class_validators import root_validator


class CommandlineArgumentValidator(BaseModel):
    module: str
    start_date: str
    end_date: str

    @validator("start_date", "end_date", always=True)
    def check_date_format(cls: Callable, v: str) -> datetime:
        try:
            parsed_date = datetime.strptime(v, "%Y-%m-%d")
            return parsed_date
        except ValueError:
            raise ValueError(
                f"Incorrect date format for `{v}`, the date should have format of YYYY-MM-DD"
            )

    @root_validator
    def check_start_date_is_before_end_date(cls: Callable, values: Dict) -> Dict:
        start_date, end_date = values.get("start_date"), values.get("end_date")
        if all([start_date is not None, end_date is not None, start_date > end_date]):
            raise ValueError(
                f"{start_date} > {end_date}. `start_date` must be before `end_date`"
            )
        return values
