from typing import Optional

from harness.config.SourceConfig import SourceConfig
from harness.config.SourceTypeEnum import SourceTypeEnum


class JDBCSourceConfig(SourceConfig):
    """
    Configuration for a JDBC data source.

    Attributes:
        source_table (str): The name of the table in the data source.
        source_schema (str): The schema of the data source.
        source_type (SourceTypeEnum): The type of the data source, defaults to SourceTypeEnum.databricks_jdbc.
    """

    source_table: str
    source_schema: str
    source_type: SourceTypeEnum = SourceTypeEnum.databricks_jdbc


class DatabricksTableSourceConfig(SourceConfig):
    """
    Configuration for a Databricks table data source.

    Attributes:
        source_catalog (Optional[str]): The name of the catalog containing the source table.
        source_table (str): The name of the source table.
        source_schema (str): The name of the schema containing the source table.
        source_type (SourceTypeEnum): The type of the data source (always SourceTypeEnum.dbrx for Databricks tables).
    """

    source_catalog: Optional[str]
    source_table: str
    source_schema: str
    source_type: SourceTypeEnum = SourceTypeEnum.dbrx
