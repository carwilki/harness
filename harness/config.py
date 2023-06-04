import uuid
from enum import Enum
from typing import Any, Optional
from pydantic import BaseModel


class EnumBase(Enum):
    def __eq__(self, other: Any) -> bool:
        if isinstance(other, Enum):
            return self.value == other.value
        return False


class SnapshotSource(str, EnumBase):
    jdbc = "jdbc"
    rocky = "rocky"


class RaptorConfig(BaseModel):
    email_address: list[str]
    teams_cahnnel: str
    primary_key_list: list[str]
    source_query: str
    source_system_type: str
    target_query: str
    target_system_type: str
    output_table_name_format: str


class RockyConfig(BaseModel):
    table_group: str = "NZ_Migration"
    table_group_desc: Optional[str] = None
    source_type: str = "NZ_Mako8"
    source_db: str = "EDW_PRD"
    source_table: str
    table_des: Optional[str] = None
    target_sink: str = "delta"
    target_db: str = "refine"
    target_schema: Optional[str] = None
    target_table_name: str
    snowflake_ddl: Optional[str] = None
    load_type: str = "full"
    source_delta_colunm: Optional[str] = None
    primary_key: Optional[str] = None
    initial_load_filter: Optional[str] = None
    has_hard_deletes: bool = False
    is_pii: bool = False
    pii_type: Optional[str] = None
    snowflake_pre_sql: Optional[str] = None
    snowflake_post_sql: Optional[str] = None
    additional_config: Optional[dict[str, str]] = None
    load_frequency: str = "one-time"
    load_cron_expr: Optional[str] = """0 0 6 ? * *"""
    max_retry: int = 1
    disable_no_record_failure: bool = True
    job_tag: Optional[str] = None
    is_scheduled: bool = False
    tidal_dependencies: list[str] = list(())
    tidal_trigger_condition: Optional[str] = None
    job_watchers: list[str] = list(())
    raptor_config: RaptorConfig


class SnapshotterConfig(BaseModel):
    snapshot_name: Optional[str] = None
    target_config: RockyConfig
    pre_table_configs: list[RockyConfig] = list(())


class SnapshotProperties(BaseModel):
    rocky_id: int
    version_number: int


class Snapshot(BaseModel):
    snapshot_id: str = uuid.uuid4().hex
    snapshot_name: str
    snapshot_config: SnapshotterConfig
    
