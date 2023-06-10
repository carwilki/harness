from datetime import datetime

from pydantic import BaseModel


class DataFrameValidatorReport(BaseModel):
    summary: str
    missmatch_sample: str
    validation_date: datetime