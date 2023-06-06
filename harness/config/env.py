from os import environ as env
from typing import Optional

class PetSmartEnvConfig:
    workspace_url: str = env.get("WORKSPACE_URL")
    workspace_access_token = env.get("WORKSPACE_ACCESS_TOKEN")
    rocky_job_id = env.get("ROCKY_JOB_ID")
    raptor_job_id = env.get("RAPTOR_JOB_ID")

    def __init__(self, env: Optional[dict[str, any]]):
        self._env = env
        if env is not None:
            for key, value in env.items():
                env[key] = value
