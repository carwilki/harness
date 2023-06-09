import abc
from typing import Optional
from pydantic import BaseModel
from harness.utils.enumbase import EnumBase
from datetime import datetime


class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"
    dataframe = "dataframe"


class SourceTypeEnum(EnumBase):
    rocky = "rocky"
    jdbc = "jdbc"


class TargetTypeEnum(EnumBase):
    rocky = "rocky"
    dbrtable = "dbrtable"


class SourceConfig(BaseModel, abc.ABC):
    source_type: SourceTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum
    validator: Optional[ValidatorTypeEnum] = None


class SnapshotConfig(BaseModel):
    name: Optional[str] = None
    target: TargetConfig
    source: SourceConfig
    version: int = 1
    validator: Optional[ValidatorTypeEnum] = None
    validated: bool = False
    validation_date: Optional[datetime] = None
    

class HarnessJobConfig(BaseModel):
    job_id: str
    snapshot_name: Optional[str] = None
    sources: dict[str, SnapshotConfig]
    inputs: dict[str, SnapshotConfig]


class HarnessMetadata(BaseModel):
    job_id: str
    config: HarnessJobConfig
    running: bool = False
    last_run: Optional[datetime] = None