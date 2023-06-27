from typing import Optional

from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum


class JDBCSourceConfig(SourceConfig):
    source_filter: Optional[str]
    source_table: str
    source_schema: str
    source_type: SourceTypeEnum = SourceTypeEnum.databricks_jdbc
