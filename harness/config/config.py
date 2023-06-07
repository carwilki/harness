import abc
from typing import Any, Dict, Optional
from pydantic import BaseModel
from harness.utils.enumbase import EnumBase
from collections.abc import Sequence

class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"


class SourceTypeEnum(EnumBase):
    netezza = "netezza"
    rocky = "rocky"


class TargetTypeEnum(EnumBase):
    delta = "delta"


class ValidatorConfig(BaseModel, abc.ABC):
    validator_type: ValidatorTypeEnum
    config: dict[str, Any]


class SourceConfig(BaseModel, abc.ABC):
    source_type: SourceTypeEnum
    config: dict[str, Any]


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum
    config: dict[str, Any]
    validator: Optional[ValidatorConfig] = None


class SnapshotConfig(BaseModel):
    snapshot_name: Optional[str] = None
    sources: dict[str, tuple[SourceConfig, TargetConfig]]
    inputs: dict[str, tuple[SourceConfig, TargetConfig]]
