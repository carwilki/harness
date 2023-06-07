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
    rocky = "rocky"


class ConfigBase(BaseModel, abc.ABC):
    config: dict[str, Any]


class ValidatorConfig(ConfigBase, abc.ABC):
    validator_type: ValidatorTypeEnum


class SourceConfig(ConfigBase, abc.ABC):
    source_type: SourceTypeEnum


class TargetConfig(BaseModel, abc.ABC):
    target_type: TargetTypeEnum
    validator: Optional[ValidatorConfig] = None


class SnapshotConfig(BaseModel):
    snapshot_name: Optional[str] = None
    sources: dict[str, tuple[SourceConfig, TargetConfig]]
    inputs: dict[str, tuple[SourceConfig, TargetConfig]]
