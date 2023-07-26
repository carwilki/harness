from datetime import datetime
from typing import Optional

from pydantic import BaseModel


class DataFrameValidatorReport(BaseModel):
    summary: str
    table: str
    validation_date: datetime
    base_only: Optional[str] = None
    compare_only: Optional[str] = None
    missmatch_only: Optional[str] = None

    @classmethod
    def empty(cls):
        return DataFrameValidatorReport(
            summary="No data in master and canidate",
            table="no_data",
            validation_date=datetime.now(),
        )
