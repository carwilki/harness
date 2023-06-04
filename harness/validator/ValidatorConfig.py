from typing import Any
from pydantic import BaseModel

from utils.enumbase import EnumBase

class ValidatorTypeEnum(EnumBase):
    raptor = "raptor"

class ValidatorConfig(BaseModel):
    def __init__(self, config):
        self.type:ValidatorTypeEnum = config.type
        self.config:ValidatorConfig = config