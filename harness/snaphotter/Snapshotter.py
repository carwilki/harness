from logging import getLogger

from harness.config.SnapshotConfig import SnapshotConfig
from harness.snaphotter.AbstractSnapshotter import AbstractSnapshotter
from harness.sources.AbstractSource import AbstractSource
from harness.target.AbstractTarget import AbstractTarget
from harness.validator.DataFrameValidator import DataFrameValidator


class Snapshotter(AbstractSnapshotter):
    def __init__(
        self, config: SnapshotConfig, source: AbstractSource, target: AbstractTarget
    ) -> None:
        super().__init__(config=config, source=source, target=target)
        self._logger = getLogger()
        if self.config.validator is not None:
            self._validator = DataFrameValidator(self.config.validator)

    def take_snapshot(self):
        if self.config.version < 0:
            self.config.version = 0
            self._logger.info("invalid version number provided, setting version to 0")

        self._logger.info(
            f"Snapshotting {self.config.name} version {self.config.version}"
        )

        if self.config.version <= 1:
            df = self.source.read()
            self.target.write(df)
            self.config.version += 1
            if self.config.validator is not None:
                self._validator.validate(df, self.source.read, self.source.session)
