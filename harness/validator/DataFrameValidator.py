from datetime import datetime
from io import StringIO

from datacompy import SparkCompare
from pyspark.sql import DataFrame, SparkSession

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
        # this is a bit of a hack, but we need to rename any '_base' columns to
        # to something else since base is a reserved word
        master_new = self.rename_base_colunms(master).localCheckpoint()
        canidate_new = self.rename_base_colunms(canidate).localCheckpoint()
        
        cc = canidate.count()
        mc = master.count()
        
        if cc == 0 and mc == 0:
            self._logger.info(f"No data to validate for {name}")
            self._logger.info("skipping validation .....")
            return DataFrameValidatorReport.empty()

        comparison = SparkCompare(
            cache_intermediates=True,
            spark_session=session,
            base_df=master_new,
            compare_df=canidate_new,
            join_columns=primary_keys,
        )

        report_table_name = f"{HarnessJobManagerEnvironment.snapshot_schema()}.{name}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"  # noqa: E501

        comparison_result = StringIO()
        self._logger.info(f"Comparing {name} master and canidate")
        comparison.report(comparison_result)
        self._logger.info("Writing report tables")

        comparison.rows_only_compare.write.saveAsTable(
            f"{report_table_name}_compare_only"
        )
        comparison.rows_only_base.write.saveAsTable(f"{report_table_name}_base_only")

        comparison.rows_both_mismatch.write.saveAsTable(
            f"{report_table_name}_missmatch_only"
        )

        self._logger.info(f"compare only: {report_table_name}_compare_only")
        self._logger.info(f"base only: {report_table_name}_base_only")
        self._logger.info(f"mismatch only: {report_table_name}_missmatch_only")
        self._logger.info(f"summary for {name}:")
        self._logger.info(comparison_result.getvalue())

        return DataFrameValidatorReport(
            summary=comparison_result.getvalue(),
            table=report_table_name,
            validation_date=datetime.now(),
        )

    def rename_base_colunms(self, df: DataFrame) -> DataFrame:
        for feild in df.schema.fields:
            if feild.name.lower().endswith("_base"):
                self._logger.info(f"Renaming {feild.name}")
                df = df.withColumnRenamed(
                    feild.name, feild.name.lower().replace("_base", "_base_sfx")
                )
        return df
