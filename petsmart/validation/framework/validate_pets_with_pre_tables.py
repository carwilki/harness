from pyspark.sql import SparkSession

from harness.snaphotter.Snapshotter import Snapshotter
from harness.target.TableTargetConfig import TableTargetConfig
from harness.validator.DataFrameValidatorReport import DataFrameValidatorReport


def validate_pets_with_pre_table(
    snapshot: Snapshotter,
    spark: SparkSession,
    key_overides: list[str] | None = None,
    pre_keys_overide: list[str] | None = None,
) -> str:
    target: TableTargetConfig = snapshot.config.target
    v1snapshot = snapshot.target.getSnapshotTableName(1)
    v2snapshot = snapshot.target.getSnapshotTableName(2)
    filter = "location_id in(1288,1186)"
    pre_filter = "dc_nbr in(36,38)"
  
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

    refine_full = spark.sql(
        f"select * from {target.test_target_schema}.{target.test_target_table} where {filter}"
    ).cache()

    refine_keys = refine_full.select(keys).repartition(25).cache()

    v1 = spark.sql(f"select * from {v1snapshot} where {filter}").cache()

    v2 = spark.sql(f"select * from {v2snapshot} where {filter}").cache()

    v1_keys = v1.select(keys).cache()

    v2_keys = v2.select(keys).cache()

    pre_full = spark.sql(
        f"select * from qa_raw.{target.test_target_table}_PRE where {pre_filter}"
    )
    
    pre_post_join = pre_full.join(site_profile, pre_full.DC_NBR == site_profile.STORE_NBR).select(pre_full["*"],
                                                                                                  site_profile["location_id"])

    pre_keys = pre_post_join.select(raw_keys)


    report: DataFrameValidatorReport = None
    report: DataFrameValidatorReport = snapshot.validateResults()

    vA = v1_keys.unionByName(v2_keys).distinct().cache()
    v1_only = v1_keys.exceptAll(v2_keys).cache()
    v2_only = v2_keys.exceptAll(v1_keys).cache()
    v1_v2_shared = vA.exceptAll(v1_only).exceptAll(v2_only).cache()
    v2_missing_from_pre = v2_only.exceptAll(pre_keys)
    new_inserts = refine_keys.exceptAll(v1_keys).cache()
    records_in_pre_not_in_refine = pre_keys.exceptAll(refine_keys).cache()
    records_in_v1_not_in_refine = v1_only.exceptAll(refine_keys).cache()
    records_in_v2_not_in_refine = v2_only.exceptAll(v2_missing_from_pre).exceptAll(refine_keys).cache()
    new_records = pre_keys.exceptAll(v1_keys).cache()
    new_records_in_refine_but_not_pre = refine_keys.exceptAll(new_inserts).exceptAll(v2_keys).exceptAll(v1_keys)
    v2_in_refine = v2_keys.exceptAll(v2_missing_from_pre)
    ret = "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    ret+=("start of validation report\n")
    ret+=(f"\n{report.summary}\n")
    ret+=(f"pre-table records:                                  {pre_full.count()}\n")
    ret+=(f"pre-keys post join records:                         {pre_post_join.count()}\n")
    ret+=(f"unique_v1:                                          {v1_only.count()}\n")
    ret+=(f"unique_v2:                                          {v2_only.count()}\n")
    ret+=(f"v1_v2_shared_records:                               {v1_v2_shared.count()}\n")
    ret+=(f"all versions(v1 & v2):                              {vA.count()}\n")
    ret+=("\n")
    ret+=(f"records already in refine:                          {v1.count()}\n")
    ret+=(f"new records in pre-table:                           {new_records.count()}\n")
    ret+=(f"expected inserted records:                          {new_records.count()}\n")
    ret+=(f"records inserted into refine:                       {new_inserts.count()}\n")
    ret+=(f"records in refine:                                  {refine_keys.count()}\n")
    ret+=(f"master records not in pre:                          {v2_missing_from_pre.count()}\n")    
    ret+=(f"master records in refine:                           {v2_in_refine.count()}\n")
    ret+=(f"v1 records missing in refine:                       {records_in_v1_not_in_refine.count()}\n")
    ret+=(f"master not in pre and missing from refine:          {records_in_v2_not_in_refine.count()}\n")
    ret+=(f"new records from pre not in refine:                 {records_in_pre_not_in_refine.count()}\n")
    ret+=(f"new records in refine but not pre:                  {new_records_in_refine_but_not_pre.count()}\n")
    ret+=("end of validation report")
    ret+=(
        "@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@@\n"
    )

    return ret
