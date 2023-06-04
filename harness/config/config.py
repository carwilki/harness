from abc import abstractclassmethod
import uuid
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel

from harness.utils.enumbase import EnumBase

class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"
    
class SourceConfig(BaseModel):
    type:ValidatorTypeEnum
    config:ValidatorConfig

class TargetConfig(BaseModel):
    type:ValidatorTypeEnum
    config:ValidatorConfig

class SnapshotConfig(SourceConfig,TargetConfig,BaseModel):
    snapshot_name: Optional[str] = None
    source: SourceConfig
    source_inputs: dict[str,SourceConfig] = dict(())
    target: TargetConfig
    target_inputs: dict[str,TargetConfig] = dict(()) 
    source_validator: Optional[ValidatorConfig] = None
    source_validator_inputs: dict[str,ValidatorConfig] = dict(())
       



class SnapshotProperties(BaseModel):
    rocky_id: int
    version_number: int




class ValidatorConfig(BaseModel):
    def __init__(self, config):
        self.type:ValidatorTypeEnum = config.type
        self.config:ValidatorConfig = config
