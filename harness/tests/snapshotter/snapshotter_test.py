from harness.snaphotter.snapshotter import AbstractSnapshotter
from harness.tests.utils.generator import generate_snapshot_config
from pyspark.sql import SparkSession


class TestSnapshotter:
    def test_creation(self, mocker):
        config = generate_snapshot_config()
        source = mocker.MagicMock().read.return_value(True)
        target = mocker.MagicMock().write.return_value(True)
        snapshotter = AbstractSnapshotter(
            config=config,
            source=source,
            target=target,
        )
        assert snapshotter.config == config

    def test_take_snapshot(self, mocker):
        session: SparkSession = mocker.MagicMock()
        source = mocker.MagicMock().read.return_value(True)
        target = mocker.MagicMock().write.return_value(True)
        config = generate_snapshot_config()
        snapshotter = AbstractSnapshotter(
            config=config,
            source=source,
            target=target,
        )
    
        snapshotter.take_snapshot(version="1", session=session)
