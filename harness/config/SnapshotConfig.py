from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from harness.config.SourceConfig import SourceConfig
from harness.config.TargetConfig import TargetConfig
from harness.config.ValidatorTypeEnum import ValidatorTypeEnum


class SnapshotConfig(BaseModel):
    name: Optional[str] = None
    target: TargetConfig
    source: SourceConfig
    version: int = 1
    validator: Optional[ValidatorTypeEnum] = None
    validated: bool = False
    validation_date: Optional[datetime] = None
    validation_report: Optional[str] = None