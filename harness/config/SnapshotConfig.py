from datetime import datetime
from typing import Optional

from pydantic import BaseModel,validator

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
    @validator("target")
    def valid_target(cls,value):
        if value is None or isinstance(value,TargetConfig) == False:
            raise ValueError("target provided is null or it's of incorrect datatype")
        return value
    
    @classmethod
    @validator("source")
    def valid_source(cls,value):
        if value is None or isinstance(value,SourceConfig) == False:
            raise ValueError("source provided is null or it's of incorrect datatype")
        return value
    
    @classmethod
    @validator("validated")
    def valid_validated(cls,value):
        if value is None or isinstance(value,bool) == False:
            raise ValueError("validated provided is null or it's of incorrect datatype")
        return value
