from datetime import datetime

from pydantic import BaseModel


class DataFrameValidatorReport(BaseModel):
    summary: str
    table: str
    validation_date: datetime

    @classmethod
    def empty(cls):
        return DataFrameValidatorReport(
            summary="No data in master and canidate",
            table="no_data",
            validation_date=datetime.now(),
        )
