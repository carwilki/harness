import abc
from typing import Optional

from pydantic import BaseModel, validator

from harness.config.TargetTypeEnum import TargetTypeEnum
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum

    @classmethod
    @validator("target_type")
    def valid_source(cls, value):
        if value is None or isinstance(value, TargetTypeEnum) == False:
            raise ValueError(
                "target_type provided is null or it's of incorrect datatype"
            )
        return value
