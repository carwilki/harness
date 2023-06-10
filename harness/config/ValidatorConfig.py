from typing import Optional

from pydantic import BaseModel


class ValidatorConfig(BaseModel):
    join_keys: list[str]
    filter: Optional[str] = None
    validator_reports: Optional[dict[str, str]] = None
