from harness.config.config import SourceTypeEnum
from harness.snaphotter.rocky.rocky_config import RockySnapshotConfig


class TestRockySourceConfig:
    def test_type(self):
        config = RockySnapshotConfig(
            config={},
            source_table="test_table1",
            rocky_target_table_name="test_table2",
            target_table_name="test_table3",
        )
        # assert the the variables are set correctly
        assert config.source_type == SourceTypeEnum.rocky
        assert config.source_table == "test_table1"
        assert config.rocky_target_table_name == "test_table2"
        assert config.target_table_name == "test_table3"
        # assert that the defualt values are set correctly
        assert config.table_group == "NZ_Migration"
        assert config.source_db_type == "NZ_Mako4"
        assert config.source_db == "EDW_PRD"
        assert config.target_sink == "delta"
        assert config.rocky_target_db == "qa_raw"
        assert config.target_db == "qa_refine"
        assert config.load_type == "full"
        assert not config.has_hard_deletes
        assert not config.is_pii
        assert config.load_frequency == "one-time"
        assert config.load_cron_expr == """0 0 6 ? * *"""
        assert config.max_retry == 1
        assert config.disable_no_record_failure
        assert not config.is_scheduled
        assert config.job_watchers == list(())
        assert config.job_id is None
        assert config.rocky_id is None
