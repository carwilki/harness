"""
module for rocky config classes
"""

from typing import Optional

from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum
from harness.config.TargetConfig import TargetConfig
from harness.config.TargetTypeEnum import TargetTypeEnum


class RockySnapshotConfig(SourceConfig, TargetConfig):
    """
    Defines a rocky source config to be used when the source type is rocky

    Args:
        SourceConfig (_type_): Base class for source configs
        BaseModel (_type_): Base class for pydantic models
    """

    target_type = TargetTypeEnum.rocky
    source_type = SourceTypeEnum.rocky
    job_id: Optional[str] = None
    rocky_id: Optional[str] = None
    table_group: str = "NZ_Migration"
    source_db_type: str = "NZ_Mako4"
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
