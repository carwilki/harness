from datetime import datetime
from io import StringIO

from datacompy import SparkCompare
from pyspark.sql import DataFrame, SparkSession

from harness.config.ValidatorConfig import ValidatorConfig
from harness.manager.HarnessJobManagerEnvironment import \
    HarnessJobManagerEnvironment
from harness.utils.logger import getLogger
from harness.validator.AbstractValidator import AbstractValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class DataFrameValidator(AbstractValidator):
    def __init__(self, config: ValidatorConfig):
        self._logger = getLogger()
        self._config = config

    def validateDF(
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
        report_table_name = f"{HarnessJobManagerEnvironment.snapshot_schema()}.{name}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"

        missmatch_both.write.saveAsTable(report_table_name)

        return DataFrameValidatorReport(
            summary=comparison_result.getvalue(),
            table=report_table_name,
            validation_date=datetime.now(),
        )

    def validate(self) -> DataFrameValidatorReport:
        """
        Validate the data frame
        Args:
            df (DataFrame): Data frame to validate
        """
        comparison = SparkCompare(
            spark_session=self._session,
            base_df=self._getDataframeFrom(
                self._config.base_schema, self._config.base_table, self._session
            ),
            compare_df=self._getDataframeFrom(
                self._config.canidate_schema, self._config.canidate_table, self._session
            ),
            join_columns=self._config.join_keys,
        )

        comparison_result = StringIO()
        comparison.report(comparison_result)
        missmatch_both: DataFrame = comparison.rows_both_mismatch
        report_table_name = f"""{HarnessJobManagerEnvironment.snapshot_schema()}.{self._config.base_schema}_{self._config.base_table}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"""  # noqa: E501

        missmatch_both.write.saveAsTable(report_table_name)

        return DataFrameValidatorReport(
            summary=comparison_result.getvalue(),
            table=report_table_name,
            validation_date=datetime.now(),
        )

    def _getDataframeFrom(
        self, source_schema: str, source_table: str, session: SparkSession
    ) -> DataFrame:
        return SparkSession.sql(f"SELECT * FROM {source_schema}.{source_table}")
