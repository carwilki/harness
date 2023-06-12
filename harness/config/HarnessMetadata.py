from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

from harness.config.HarnessJobConfig import HarnessJobConfig


class HarnessMetadata(BaseModel):
    job_id: str
    config: HarnessJobConfig
    running: bool = False
    last_run: Optional[datetime] = None

    @classmethod
    @validator("job_id")
    def valid_job_id(cls, value):
        if value is None or isinstance(value, str) == False:
            raise ValueError("Job ID provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("config")
    def valid_config(cls, value):
        if value is None or isinstance(value, HarnessJobConfig) == False:
            raise ValueError("config provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("running")
    def valid_running(cls, value):
        if value is None or isinstance(value, bool) == False:
            raise ValueError("config provided is null or it's of incorrect datatype")
        return value
