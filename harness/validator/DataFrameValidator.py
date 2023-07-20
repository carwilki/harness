from datetime import datetime
from io import StringIO

from datacompy import SparkCompare
from pyspark.sql import DataFrame, SparkSession

from harness.config.ValidatorConfig import ValidatorConfig
from harness.manager.HarnessJobManagerEnvironment import HarnessJobManagerEnvironment
from harness.utils.logger import getLogger
from harness.validator.AbstractValidator import AbstractValidator
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


class DataFrameValidator(AbstractValidator):
    def __init__(self):
        self._logger = getLogger()

    def validateDF(
        self,
        name: str,
        canidate: DataFrame,
        master: DataFrame,
        primary_keys: list[str],
        session: SparkSession,
    ) -> DataFrameValidatorReport:
        """
        Validate the data frame
        Args:000
            df (DataFrame): Data frame to validate
        """
        cc = canidate.count()
        mc = master.count()
        if cc == 0 and mc == 0:
            self._logger.info(f"No data to validate for {name}")
            self._logger.info("skipping validation .....")
            return DataFrameValidatorReport.empty()

        comparison = SparkCompare(
            cache_intermediates=True,
            spark_session=session,
            base_df=master,
            compare_df=canidate,
            join_columns=primary_keys,
        )

        report_table_name = f"{HarnessJobManagerEnvironment.snapshot_schema()}.{name}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"  # noqa: E501
        
        comparison_result = StringIO()
        self._logger.info(f"Comparing {name} master and canidate")
        comparison.report(comparison_result)
        missmatch_both: DataFrame = comparison.rows_both_mismatch

        self._logger.info(f"Writing report to {report_table_name}")
        missmatch_both.write.saveAsTable(report_table_name)

        return DataFrameValidatorReport(
            summary=comparison_result.getvalue(),
            table=report_table_name,
            validation_date=datetime.now(),
        )
