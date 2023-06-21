from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class ValidatorConfig(BaseModel):
    join_keys: list
    filter: Optional[str] = None
    validator_reports: Optional[dict[datetime, DataFrameValidatorReport]] = None

    @classmethod
    @validator("join_keys")
    def valid_join_keys(cls, value):
        if value is None or isinstance(value, list) is False:
            raise ValueError("join_keys provided is null or it's of incorrect datatype")
        return value
