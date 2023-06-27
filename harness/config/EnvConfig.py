from typing import Optional

from pydantic import BaseModel


class EnvConfig(BaseModel):
    workspace_url: str
    workspace_token: str
    catalog: Optional[str] = None
    metadata_schema: str
    metadata_table: str
    snapshot_schema: str
    # TODO: add support for multiple snapshot schemas
    # each source should have its own jdbc url and configuration.
    # these are used for defaults for the source if not provided
    # Netezza JDBC Configuration Options ###
    netezza_jdbc_url: Optional[str] = None
    netezza_jdbc_user: Optional[str] = None
    netezza_jdbc_password: Optional[str] = None
    netezza_jdbc_num_part: Optional[int] = None
    netezza_jdbc_driver: Optional[str] = None
    # Databricks JDBC Configuration Options ###
    databricks_jdbc_host: Optional[str] = None
    databricks_jdbc_http_path: Optional[str] = None
    databricks_jdbc_user: Optional[str] = None
    databricks_jdbc_pat: Optional[str] = None
