from datetime import datetime
from utils.logger import getLogger

from harness.config.SnapshotConfig import SnapshotConfig
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.sources.JDBCSource import JDBCSource
from harness.target.TableTarget import TableTarget
from harness.validator.DataFrameValidator import DataFrameValidator


class Snapshotter(AbstractSnapshotter):
    def __init__(
        self, config: SnapshotConfig, source: JDBCSource, target: TableTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)
        self._logger = getLogger()
        if self.config.validator is not None:
            self._validator = DataFrameValidator(self.config.validator)
        else:
            self._validator = None
            self._logger.info(
                f"No validator configured for snapshotter {self.config.name}"
            )
                    
    def snapshot(self):
        if self.config.version < 0:
            self.config.version = 0
            self._logger.info("invalid version number provided, setting version to 0")

        if self.config.version < 1:
            self._logger.info(f"Taking snapshot V1 of {self.config.name}...")
            self._snapshot()
        elif self.config.version == 1:
            self._logger.info(f"Taking snapshot V2 of {self.config.name}...")
            self._snapshot()
        else:
            self._logger.info(
                f"V2 snapshot of {self.config.name} detected, skipping..."
            )

    def _snapshot(self):
        df = self.source.read()
        self.target.write(df, self.config.job_id)
        if self.config.validator is not None:
            date = datetime.now().strftime("%Y-%m-%d %H:%M")
            report = self._validator.validateDF(
                name=self.config.name,
                master=self.source.read(),
                canidate=df,
                session=self.source.session,
            )
            if self.config.validator.validator_reports is None:
                self.config.validator.validator_reports = {}
                
            self.config.validator.validator_reports[date] = report

        self.config.version += 1
