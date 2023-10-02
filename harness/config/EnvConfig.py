from typing import Optional

from pydantic import BaseModel


class EnvConfig(BaseModel):
    """
    A configuration class that holds environment variables for a workspace.

    Attributes:
        workspace_url (str): The URL of the workspace.
        workspace_token (str): The token used to authenticate with the workspace.
        metadata_schema (str): The schema used for metadata.
        metadata_table (str): The table used for metadata.
        snapshot_schema (str): The schema used for snapshots.
        netezza_jdbc_url (Optional[str]): The JDBC URL for Netezza.
        netezza_jdbc_user (Optional[str]): The username for Netezza.
        netezza_jdbc_password (Optional[str]): The password for Netezza.
        netezza_jdbc_num_part (Optional[int]): The number of partitions for Netezza.
        netezza_jdbc_driver (Optional[str]): The JDBC driver for Netezza.
        databricks_jdbc_host (Optional[str]): The host for Databricks.
        databricks_jdbc_http_path (Optional[str]): The HTTP path for Databricks.
        databricks_jdbc_user (Optional[str]): The username for Databricks.
        databricks_jdbc_pat (Optional[str]): The personal access token for Databricks.
    """

    workspace_url: str
    workspace_token: str
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
