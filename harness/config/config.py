from abc import abstractclassmethod
import uuid
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel

from harness.utils.enumbase import EnumBase


class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"


class SourceTypeEnum(EnumBase):
    netezza = "netezza"


class TargetTypeEnum(EnumBase):
    delta = "delta"


class ValidatorConfig(BaseModel):
    type: ValidatorTypeEnum
    config: dict[str, Any]


class SourceConfig(BaseModel):
    config: dict[str, Any]


class TargetConfig(BaseModel):
    config: dict[str, Any]
    validator: Optional[ValidatorConfig] = None


class SnapshotConfig(SourceConfig, TargetConfig, BaseModel):
    snapshot_name: Optional[str] = None
    source: SourceConfig
    source_inputs: dict[str, (SourceConfig, TargetConfig)] = dict(())
    target_inputs: dict[str, (SourceConfig, TargetConfig)] = dict(())
