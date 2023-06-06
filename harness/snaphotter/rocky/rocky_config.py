from typing import Optional
from pydantic import BaseModel

from harness.config import RaptorConfig
from harness.config.config import SnapshotConfig
from harness.validator import validator_config


class RockyConfig(SnapshotConfig, BaseModel):
    job_id: Optional[str] = None
    rocky_id: Optional[str] = None
    table_group: str = "NZ_Migration"
    source_type: str = "NZ_Mako4"
    source_db: str = "EDW_PRD"
    source_table: str
    target_sink: str = "delta"
    rocky_target_db: str = "qa_raw"
    rocky_target_table_name: str
    target_sink: str = "delta"
    target_db: str = "qa_refine"
    target_table_name: str
    load_type: str = "full"
    has_hard_deletes: bool = False
    is_pii: bool = False
    load_frequency: str = "one-time"
    load_cron_expr: Optional[str] = """0 0 6 ? * *"""
    max_retry: int = 1
    disable_no_record_failure: bool = True
    is_scheduled: bool = False
    job_watchers: list[str] = list(())
