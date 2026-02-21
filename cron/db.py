import ydb.iam

import os
import json
from time import time
from utils import get_when

import dotenv


dotenv.load_dotenv()

driver = ydb.Driver(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    # credentials=ydb.AuthTokenCredentials(os.getenv('IAM_TOKEN'))
    credentials=ydb.iam.MetadataUrlCredentials(),
)

driver.wait(fail_fast=True, timeout=20)

pool = ydb.SessionPool(driver)

settings = ydb.BaseRequestSettings().with_timeout(30).with_operation_timeout(20)


def execute(yql):
    def wrapper(session):
        try:
            res = session.transaction().execute(yql, commit_tx=True, settings=settings)
            return res[0].rows if len(res) else []

        except Exception as e:
            print(e)
            return []

    print(yql)
    return pool.retry_operation_sync(wrapper)


def read_crons():
    now = int(time())
    crons = execute(
        f"SELECT id, group_id, thread_id, memo, trigger, time_zone FROM crons WHERE create<={now} AND create>0;"
    )
    for c in crons:
        c["trigger"] = json.loads(c["trigger"])
    return crons


def update_when(cron):
    when = get_when(cron["trigger"], cron["time_zone"])
    execute(f"UPDATE crons SET create={when} WHERE id={cron['id']};")
