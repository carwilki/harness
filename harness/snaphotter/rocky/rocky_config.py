"""
module for rocky config classes
"""

from typing import Optional
from pydantic import BaseModel
from harness.config.config import (
    SourceConfig,
    SourceTypeEnum,
    TargetConfig,
    TargetTypeEnum,
)


class RockySourceConfig(SourceConfig):
    """
    Defines a rocky source config to be used when the source type is rocky

    Args:
        SourceConfig (_type_): Base class for source configs
        BaseModel (_type_): Base class for pydantic models
    """
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


class RockyTargetConfig(TargetConfig):
    """
    Defines a rocky target config to be used when the target type is rocky
    this config is just really a place holder since the rocky target config is
    basically the same as the rocky source config + an options validator

    Args:
        TargetConfig (_type_): base class for target configs
    """

    target_type = TargetTypeEnum.rocky
