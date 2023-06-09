from typing import Optional


class EnvConfig:
    workspace_url: str
    workspace_token: str
    catalog: Optional[str] = None
    metadata_schema: str
    metadata_table: str
