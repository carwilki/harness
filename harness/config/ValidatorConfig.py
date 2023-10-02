from datetime import datetime
from typing import Optional

from pydantic import BaseModel, validator

from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class ValidatorConfig(BaseModel):
    """
    A configuration object for validating data in two tables.

    Attributes:
        base_schema (str): The schema name of the base table.
        base_table (str): The name of the base table.
        canidate_schema (str): The schema name of the candidate table.
        canidate_table (str): The name of the candidate table.
        join_keys (list): A list of column names to join the tables on.
        filter (Optional[str]): A filter to apply to the data before joining.
        validator_reports (Optional[dict[datetime, DataFrameValidatorReport]]): A dictionary of validation reports.
    """

    base_schema: str
    base_table: str
    canidate_schema: str
    canidate_table: str
    join_keys: list
    filter: Optional[str] = None
    validator_reports: Optional[dict[datetime, DataFrameValidatorReport]] = None

    @classmethod
    @validator("join_keys")
    def valid_join_keys(cls, value):
        if value is None or isinstance(value, list) is False:
            raise ValueError("join_keys provided is null or it's of incorrect datatype")
        return value
