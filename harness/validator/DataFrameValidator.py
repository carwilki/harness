from datetime import datetime
from io import StringIO

from datacompy import SparkCompare
from pyspark.sql import DataFrame, SparkSession

from harness.config.ValidatorConfig import ValidatorConfig
from harness.validator.AbstractValidator import AbstractValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class DataFrameValidator(AbstractValidator):
    def __init__(self, config: ValidatorConfig):
        self._config = config

    def validate(
        self, name: str, canidate: DataFrame, master: DataFrame, session: SparkSession
    ) -> DataFrameValidatorReport:
        """
        Validate the data frame
        Args:
            df (DataFrame): Data frame to validate
        """
        comparison = SparkCompare(
            spark_session=session,
            base_df=master,
            compare_df=canidate,
            join_columns=self._config.join_keys,
        )
        
        comparison_result = StringIO()
        comparison.report(comparison_result)
        missmatch_both: DataFrame = comparison.rows_both_mismatch
        report_table_name = (
            f"{name}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"
        )

        missmatch_both.write.saveAsTable(report_table_name)

        return DataFrameValidatorReport(
            summary=comparison_result,
            table=report_table_name,
            validation_date=datetime.now(),
        )
