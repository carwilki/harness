from pydantic import BaseModel

from harness.validator import validator_config


class RaptorConfig(BaseModel,validator_config):
    email_address: list[str]
    teams_cahnnel: str
    primary_key_list: list[str]
    source_query: str
    source_system_type: str
    target_query: str
    target_system_type: str
    output_table_name_format: str