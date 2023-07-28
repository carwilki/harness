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
        # this is a bit of a hack, but we need to rename any '_base' columns to
        # to something else since base is a reserved word
        master_new = self.rename_base_colunms(master).localCheckpoint()
        canidate_new = self.rename_base_colunms(canidate).localCheckpoint()

        cc = canidate.count()
        mc = master.count()

        if cc == 0 and mc == 0:
            self._logger.debug(f"No data to validate for {name}")
            self._logger.debug("skipping validation .....")
            return DataFrameValidatorReport.empty()

        comparison = SparkCompare(
            cache_intermediates=True,
            spark_session=session,
            base_df=master_new,
            compare_df=canidate_new,
            join_columns=primary_keys,
            show_all_columns=True,
        )
        summary: str = f"summary for {name}:\n"
        summary += (
            "********************************************************************\n\n"
        )

        report_table_name = f"{HarnessJobManagerEnvironment.snapshot_schema()}.{name}_validation_report_on_{datetime.now().strftime('%Y_%m_%d_%H_%M')}"  # noqa: E501

        comparison_result = StringIO()
        self._logger.debug(f"Comparing {name} master and canidate")
        comparison.report(comparison_result)
        self._logger.debug("Writing report tables")
        compare_only = f"{report_table_name}_compare_only"
        base_only = f"{report_table_name}_base_only"
        missmatch_only = f"{report_table_name}_missmatch_only"
        comparison.rows_only_compare.write.saveAsTable(compare_only)
        comparison.rows_only_base.write.saveAsTable(base_only)
        comparison.rows_both_mismatch.write.saveAsTable(missmatch_only)

        summary += f"compare only: {report_table_name}_compare_only\n"
        summary += f"base only: {report_table_name}_base_only\n"
        summary += f"mismatch only: {report_table_name}_missmatch_only\n"
        summary += comparison_result.getvalue() + "\n\n"
        summary += f"end summary: {name}\n"
        summary += (
            "********************************************************************\n"
        )
        return DataFrameValidatorReport(
            summary=summary,
            table=report_table_name,
            validation_date=datetime.now(),
            base_only=base_only,
            compare_only=compare_only,
            missmatch_only=missmatch_only,
        )

    def rename_base_colunms(self, df: DataFrame) -> DataFrame:
        for feild in df.schema.fields:
            if feild.name.lower().endswith("_base"):
                self._logger.debug(f"Renaming {feild.name}")
                df = df.withColumnRenamed(
                    feild.name, feild.name.lower().replace("_base", "_base_sfx")
                )
        return df
