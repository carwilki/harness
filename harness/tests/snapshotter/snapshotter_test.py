from harness.snaphotter.snapshotter import AbstractSnapshotter
from harness.tests.utils.generator import generate_snapshot_config

from pyspark.sql import SparkSession


class TestSnapshotter:
    def test_config(self):
        config = generate_snapshot_config()
        snapshotter = AbstractSnapshotter(config=config)
        assert snapshotter.config == config

    def test_take_snapshot(self, mocker):
        session: SparkSession = mocker.MagicMock()
        config = generate_snapshot_config()
        snapshotter = AbstractSnapshotter(config=config)
        snapshotter.take_snapshot(version="1", session=session)
