from datetime import datetime
from typing import Optional

from pydantic import BaseModel
from pydantic import validator as pydantic_validator

from harness.config.SourceConfig import SourceConfig
from harness.config.TargetConfig import TargetConfig
from harness.config.ValidatorConfig import ValidatorConfig


class SnapshotConfig(BaseModel):
    name: Optional[str] = None
    target: TargetConfig
    source: SourceConfig
    version: int = 0
    validator: Optional[ValidatorConfig] = None
    validated: bool = False
    validation_date: Optional[datetime] = None
    validation_report: Optional[str] = None

    @classmethod
    @pydantic_validator("target")
    def valid_target(cls, value):
        if value is None or isinstance(value, TargetConfig) is False:
            raise ValueError("target provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @pydantic_validator("source")
    def valid_source(cls, value):
        if value is None or isinstance(value, SourceConfig) is False:
            raise ValueError("source provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @pydantic_validator("validated")
    def valid_validated(cls, value):
        if value is None or isinstance(value, bool) is False:
            raise ValueError("validated provided is null or it's of incorrect datatype")
        return value
