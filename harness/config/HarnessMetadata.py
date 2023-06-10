from datetime import datetime
from typing import Optional

from pydantic import BaseModel

from harness.config.HarnessJobConfig import HarnessJobConfig


class HarnessMetadata(BaseModel):
    job_id: str
    config: HarnessJobConfig
    running: bool = False
    last_run: Optional[datetime] = None