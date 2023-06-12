from typing import Optional

from pydantic import BaseModel

from harness.config.SnapshotConfig import SnapshotConfig


class HarnessJobConfig(BaseModel):
    job_id: str
    version: int = 0
    snapshot_name: Optional[str] = None
    sources: dict[str, SnapshotConfig]
    inputs: dict[str, SnapshotConfig]