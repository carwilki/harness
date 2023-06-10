from typing import Optional

from pydantic import BaseModel


class EnvConfig(BaseModel):
    workspace_url: str
    workspace_token: str
    catalog: Optional[str] = None
    metadata_schema: str
    metadata_table: str
    snapshot_schema: str
    snapshot_table_post_fix: str
    jdbc_url: str
    jdbc_user: str
    jdbc_password: str
    jdbc_num_part: str
