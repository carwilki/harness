import abc
from typing import Optional
from pydantic import BaseModel
from harness.utils.enumbase import EnumBase


class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"


class SourceTypeEnum(EnumBase):
    netezza = "netezza"
    rocky = "rocky"


class TargetTypeEnum(EnumBase):
    delta = "delta"
    rocky = "rocky"


class ValidatorConfig(BaseModel, abc.ABC):
    validator_type: ValidatorTypeEnum


class SourceConfig(BaseModel, abc.ABC):
    source_type: SourceTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum
    validator: Optional[ValidatorConfig] = None


class SnapshotConfig(BaseModel):
    target: TargetConfig
    source: SourceConfig
    validator: Optional[ValidatorConfig] = None


class HarnessJobConfig(BaseModel):
    id: str
    snapshot_name: Optional[str] = None
    sources: dict[str, SnapshotConfig]
    inputs: dict[str, SnapshotConfig]
