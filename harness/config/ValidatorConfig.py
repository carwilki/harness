from typing import Optional

from pydantic import BaseModel, validator


class ValidatorConfig(BaseModel):
    join_keys: list[str]
    filter: Optional[str] = None
    validator_reports: Optional[dict[str, str]] = None

    @classmethod
    @validator("join_keys")
    def valid_source(cls, value):
        if value is None or isinstance(value, list[str]) == False:
            raise ValueError("join_keys provided is null or it's of incorrect datatype")
        return value
