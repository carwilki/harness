from harness.config.config import SourceTypeEnum, TargetTypeEnum
from harness.snaphotter.rocky.rocky_config import RockySourceConfig, RockyTargetConfig


class TestRockyTarget:
    def test_type(self):
        config = RockyTargetConfig(config={})
        assert config.target_type == TargetTypeEnum.rocky