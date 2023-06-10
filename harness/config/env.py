from typing import Optional
from pydantic import BaseModel


class EnvConfig(BaseModel):
    workspace_url: str
    workspace_token: str
    catalog: Optional[str] = None
    metadata_schema: str
    metadata_table: str