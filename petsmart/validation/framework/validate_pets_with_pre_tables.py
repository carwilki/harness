from pyspark.sql import SparkSession

from harness.snaphotter.Snapshotter import Snapshotter
from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


def validate_pets_with_pre_table(
    snapshot: Snapshotter,
    spark: SparkSession,
    key_overides: list[str] | None = None,
    pre_keys_overide: list[str] | None = None,
) -> None:
    target: TableTargetConfig = snapshot.config.target
    v1snapshot = snapshot.target.getSnapshotTableName(1)
    v2snapshot = snapshot.target.getSnapshotTableName(2)

    if key_overides is None:
        keys = target.primary_key
    else:
        keys = key_overides

    raw_keys = []
    if pre_keys_overide is None:
        for key in keys:
            raw_keys.append(key.lower().replace("wm_", ""))
    else:
        raw_keys = pre_keys_overide

    site_profile = spark.sql(
        "select STORE_NBR,LOCATION_ID from qa_legacy.SITE_PROFILE"
    ).cache()

    refine_full = (
        spark.sql(
            f"select * from {target.test_target_schema}.{target.test_target_table}"
        )
        .repartition(25)
        .distinct()
        .cache()
    )

    refine_keys = refine_full.select(keys).repartition(25).distinct().cache()

    pre_full = (
        spark.sql(f"select * from qa_raw.{target.test_target_table}_PRE")
        .distinct()
        .repartition(25)
        .cache()
    )

    pre_keys = (
        pre_full.join(site_profile, pre_full.DC_NBR == site_profile.STORE_NBR)
        .repartition(25)
        .select(raw_keys)
        .distinct()
    )

    v1 = spark.sql(f"select * from {v1snapshot}").repartition(25).distinct().cache()

    v2 = spark.sql(f"select * from {v2snapshot}").repartition(25).distinct().cache()

    v1_keys = v1.select(keys).cache()

    v2_keys = v2.select(keys).cache()

    report: DataFrameValidatorReport = None
    report: DataFrameValidatorReport = snapshot.validateResults()

    vA = v1_keys.unionByName(v2_keys).distinct().cache()
    v1_only = v1_keys.exceptAll(v2_keys).cache()
    v2_only = v2_keys.exceptAll(v1_keys).cache()
    v1_v2_shared = vA.exceptAll(v1_only).exceptAll(v2_only).cache()
    new_inserts = refine_keys.exceptAll(v1_keys).cache()
    delta_pre = pre_keys.exceptAll(v1_keys).cache()
    expected_records_inserted = delta_pre.unionAll(v2_only).exceptAll(v1_keys).cache()
    records_not_inserted = expected_records_inserted.exceptAll(refine_keys).cache()
    unexpected_records_in_refine = (
        refine_keys.exceptAll(pre_keys).exceptAll(v1_keys).exceptAll(v2_keys).cache()
    )
    records_in_pre_not_in_refine = pre_keys.exceptAll(refine_keys).cache()
    records_in_v2_not_in_refine = v2_only.exceptAll(refine_keys).cache()
    records_in_v1_not_in_refine = v1_only.exceptAll(refine_keys).cache()

    print(
        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    )
    print("start of validation report")
    print(report.summary)
    print(f"pre-table records:                                  {pre_full.count()}")
    print(f"v2 records:                                         {v2.count()}")
    print(f"all version:                                        {vA.count()}")
    print(
        f"expected inserted records:                          {expected_records_inserted.count()}"
    )
    print(f"records inserted into refine:                       {new_inserts.count()}")
    print(f"records in refine:                                  {refine_keys.count()}")
    print(
        f"records not inserted:                               {records_not_inserted.count()}"
    )
    print(
        f"unexpected records in refine:                       {unexpected_records_in_refine.count()}"
    )
    print(
        f"records in pre but not in refine:                   {records_in_pre_not_in_refine.count()}"
    )
    print(
        f"records in refine but not pre:                      {records_not_inserted.count()}"
    )
    print(
        f"records in v1 but not in refine:                    {records_in_v1_not_in_refine.count()}"
    )
    print(
        f"records in v2 but not in refine:                    {records_in_v2_not_in_refine.count()}"
    )
    print("end of validation report")
    print(
        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    )
