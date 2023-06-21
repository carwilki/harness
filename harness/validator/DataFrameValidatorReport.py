from datetime import datetime

from pydantic import BaseModel


class DataFrameValidatorReport(BaseModel):
    summary: str
    table:str
    validation_date: datetime
