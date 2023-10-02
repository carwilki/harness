from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

from harness.config.HarnessJobConfig import HarnessJobConfig


class HarnessMetadata(BaseModel):
    """
    A class representing metadata for a Harness job.

    Attributes:
        job_id (str): The ID of the job.
        config (HarnessJobConfig): The configuration for the job.
        running (bool): Whether or not the job is currently running.
        last_run (Optional[datetime]): The datetime of the last time the job was run.
    """

    job_id: str
    config: HarnessJobConfig
    running: bool = False
    last_run: Optional[datetime] = None

    @classmethod
    @validator("job_id")
    def valid_job_id(cls, value):
        if value is None or isinstance(value, str) is False:
            raise ValueError("Job ID provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("config")
    def valid_config(cls, value):
        if value is None or isinstance(value, HarnessJobConfig) is False:
            raise ValueError("config provided is null or it's of incorrect datatype")
        return value

    @classmethod
    @validator("running")
    def valid_running(cls, value):
        if value is None or isinstance(value, bool) is False:
            raise ValueError("config provided is null or it's of incorrect datatype")
        return value
