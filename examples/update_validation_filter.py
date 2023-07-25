from harness.config.EnvConfig import EnvConfig
from harness.manager.HarnessApi import HarnessApi


username = dbutils.secrets.get(scope="netezza_petsmart_keys", key="username")
password = dbutils.secrets.get(scope="netezza_petsmart_keys", key="password")
token = dbutils.secrets.get(scope="netezza_petsmart_keys", key="workspace_token")

env = EnvConfig(
    workspace_url="https://3986616729757273.3.gcp.databricks.com/",
    workspace_token=token,
    metadata_schema="nzmigration",
    metadata_table="harness_metadata_v2",
    snapshot_schema="nzmigration",
    netezza_jdbc_url="jdbc:netezza://172.16.73.181:5480/",
    netezza_jdbc_user=username,
    netezza_jdbc_password=password,
    netezza_jdbc_driver="org.netezza.Driver",
    netezza_jdbc_num_part=9,
)

api = HarnessApi(env, spark)
hjm = api.getHarnessJobById("01298d4f-934f-439a-b80d-251987f5422")
snapshots = hjm.config.snapshots.values()

# hjm.updateAllValidationFilters(filter="LOCATION_ID = 1288 or LOCATION_ID=1186")
hjm.updateValidaitonFilter(
    snapshotName="WM_E_AUD_LOG",
    filter="""where (('2023-06-12 04:39:35'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:03:00')
    or ('2023-06-16 03:03:44'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:03:00'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LABOR_MSG_DTL",
    filter="""(('2023-06-12 04:39:35'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:03:00') 
    or ('2023-06-16 03:03:44'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:03:00')) 
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LABOR_MSG_DTL_CRIT",
    filter=""""(('2023-06-12 04:45:06'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:03:33')
    or ('2023-06-16 03:04:22'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:03:33'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)"""
)
hjm.updateValidaitonFilter(
    snapshotName="WM_PIX_TRAN",
    filter="""where (('2023-06-15 02:57:19'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:01:38')
    or ('2023-06-16 03:02:10'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:01:38'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_ORDER_LINE_ITEM",
    filter="""where (('2023-06-07 03:10:36'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:03:12')
    or ('2023-06-16 03:04:18'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:03:12'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LPN_DETAIL",
    filter="""where (('2022-04-23 11:32:01'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:05:28')
    or ('2023-06-16 03:06:25'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:05:28'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_TASK_DTL",
    filter="""where (('2023-06-12 05:02:06'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:04:10')
    or ('2023-06-16 03:05:06'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:04:10'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_OUTPT_ORDER_LINE_ITEM",
    filter="""where (('2023-06-16 03:05:40'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:04:07')
    or ('2023-06-16 03:05:40'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:04:07'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_OUTPT_LPN_DETAIL",
    filter="""where (('2023-06-16 03:06:44'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:05:51')
    or ('2023-06-16 03:06:44'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:05:51'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_SLOT_ITEM_SCORE",
    filter="""where (('2023-06-16 03:03:25'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:02:18')
    or ('2023-06-16 03:03:25'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:02:18'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LABOR_MSG",
    filter="""where (('2023-05-05 03:03:59'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:02:42')
    or ('2023-06-16 03:03:31'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:02:42'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_E_EVNT_SMRY_HDR",
    filter="""where (('2023-04-28 02:54:12'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:02:12')
    or ('2023-06-16 00:00:00'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:02:12'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LPN",
    filter="""where (('2022-04-23 11:32:17'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:04:54')
    or ('2023-06-16 03:06:22'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:04:54'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_TASK_HDR",
    filter="""where (('2023-04-27 02:52:29'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:04:26')
    or ('2023-06-16 03:05:22'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:04:26'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_LABOR_MSG_CRIT",
    filter="""where (('2023-06-16 03:04:11'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:03:18')
    or ('2023-06-16 03:04:11'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:03:18'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)""",
)
hjm.updateValidaitonFilter(
    snapshotName="WM_OUTPT_LPN",
    filter="""where (('2023-06-16 03:03:26'<LOAD_TSTMP and LOAD_TSTMP <'2023-06-23 03:02:20')
    or ('2023-06-16 03:03:26'<UPDATE_TSTMP and UPDATE_TSTMP<'2023-06-23 03:02:20'))
    and (LOCATION_ID = 1288 or LOCATION_ID=1186)"""
)
