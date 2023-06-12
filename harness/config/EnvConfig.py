from typing import Optional

from pydantic import BaseModel,validator


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
    jdbc_num_part: int = 5
    jdbc_driver: str
    
    # adding pydantic validations
    @validator("workspace_url")
    @classmethod
    def valid_workspace_url(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("Workspace url is not provided or it's of incorrect datatype")
        return value
    
    @validator("workspace_token")
    @classmethod
    def valid_workspace_token(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("workspace token is not provided or it's of incorrect datatype")
        return value
    
    @validator("metadata_schema")
    @classmethod
    def valid_metadata_schema(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("metadata schema is not provided or it's of incorrect datatype")
        return value
    
    @validator("metadata_table")
    @classmethod
    def valid_metadata_table(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("metadata table is not provided or it's of incorrect datatype")
        return value
    
    @validator("snapshot_schema")
    @classmethod
    def valid_snapshot_schema(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("snapshot schema is not provided or it's of incorrect datatype")
        return value
    
    @validator("snapshot_table_post_fix")
    @classmethod
    def valid_snapshot_table_post_fix(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("snapshot_table_post_fix is not provided or it's of incorrect datatype")
        return value
    
    @validator("jdbc_url")
    @classmethod
    def valid_jdbc_url(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("jdbc url is not provided or it's of incorrect datatype")
        return value
    
    @validator("jdbc_url")
    @classmethod
    def valid_jdbc_url(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("jdbc url is not provided or it's of incorrect datatype")
        return value
    
    @validator("jdbc_password")
    @classmethod
    def valid_jdbc_password(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("jdbc password is not provided or it's of incorrect datatype")
        return value
    
    @validator("jdbc_driver")
    @classmethod
    def valid_jdbc_driver(cls,value):
        if value is None or isinstance(value,str) == False:
            return ValueError("jdbc driver is not provided or it's of incorrect datatype")
        return value
        
    
