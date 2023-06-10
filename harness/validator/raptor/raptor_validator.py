from typing import Optional

from databricks_cli.jobs.api import JobsApi
from databricks_cli.sdk.api_client import ApiClient
from manager.config import Validator
from pydantic import BaseModel


class RaptorValidatorConfig(Validator, BaseModel):
    email_address: list[str]
    teams_cahnnel: str
    primary_key_list: list[str]
    source_query: str
    source_system_type: str
    target_query: str
    target_system_type: str
    output_table_name_format: str
    host: str
    token: str


class RaptorValidator(Validator):
    _raptor_job_id = 785268334432278
    _token = "dapi80e895d3500c5b7b0f06ce6466e88d34"
    _host = "https://3247751575194472.2.gcp.databricks.com/"

    def validate(config: RaptorValidatorConfig) -> Optional[int]:
        JobsApi(ApiClient(host=self._host, token=self._token)).run_now(
            job_id=self._raptor_job_id,
            notebook_params={
                "email_address": config.email_address,
                "teams_cahnnel": config.teams_cahnnel,
                "primary_key_list": config.primary_key_list,
                "source_query": config.source_query,
                "source_system_type": config.source_system_type,
                "target_query": config.target_query,
                "target_system_type": config.target_system_type,
                "output_table_name_format": config.output_table_name_format,
            },
        )
