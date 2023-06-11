from datetime import datetime
from typing import Optional

from pydantic import BaseModel

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
