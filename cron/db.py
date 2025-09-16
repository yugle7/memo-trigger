import ydb.iam

import os
import json
from time import time

import dotenv

from utils import get_next

dotenv.load_dotenv()

driver = ydb.Driver(
    endpoint=os.getenv('YDB_ENDPOINT'),
    database=os.getenv('YDB_DATABASE'),
    credentials=ydb.AuthTokenCredentials(os.getenv('IAM_TOKEN'))
    # credentials=ydb.iam.MetadataUrlCredentials()
)

driver.wait(fail_fast=True, timeout=20)

pool = ydb.SessionPool(driver)

settings = ydb \
    .BaseRequestSettings() \
    .with_timeout(30) \
    .with_operation_timeout(20)


def execute(yql):
    def wrapper(session):
        try:
            res = session.transaction().execute(
                yql,
                commit_tx=True,
                settings=settings
            )
            return res[0].rows if len(res) else []

        except Exception as e:
            print(e)
            return []

    print(yql)
    return pool.retry_operation_sync(wrapper)


def read_crons():
    now = int(time())
    crons = execute(f'SELECT id, group_id, memo, triggers FROM crons WHERE create<={now} AND create>0;')
    for c in crons:
        c['triggers'] = json.loads(c['triggers'])
    return crons


def update_next(cron):
    t = min(get_next(t) for t in cron['triggers'])
    execute(f"UPDATE crons SET create={t} WHERE id={cron['id']};")
