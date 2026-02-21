import ydb
import ydb.iam


import os
import json

import dotenv
from utils import get_cron, get_form

dotenv.load_dotenv()

driver = ydb.Driver(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    # credentials=ydb.AuthTokenCredentials(os.getenv('IAM_TOKEN')),
    credentials=ydb.iam.MetadataUrlCredentials(),
)

driver.wait(fail_fast=True, timeout=5)

pool = ydb.SessionPool(driver)

settings = ydb.BaseRequestSettings().with_timeout(3).with_operation_timeout(2)


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


def load_data(user_id):
    forms = execute(f"SELECT form FROM forms WHERE user_id={user_id};")
    chats = execute(f"SELECT chat FROM chats WHERE user_id={user_id};")

    return {
        "forms": [json.loads(q.get("form")) for q in forms],
        "chats": [json.loads(q.get("chat")) for q in chats],
    }


def save_data(user_id, forms):
    forms = [get_form(form) for form in forms]

    values = ",".join(f"({user_id}, '{json.dumps(form)}')" for form in forms)
    execute(f"DELETE FROM forms WHERE user_id={user_id};")
    execute(f"INSERT INTO forms (user_id, form) VALUES {values};")

    crons = [get_cron(form, user_id) for form in forms]
    values = ",".join(
        f"({cron['id']}, {user_id}, {cron['group_id']}, {cron['thread_id']}, '{cron['memo']}', {cron['create']}, '{json.dumps(cron['trigger'])}', {cron['time_zone']})"
        for cron in crons
    )
    execute(f"DELETE FROM crons WHERE user_id={user_id};")
    execute(
        f"INSERT INTO crons (id, user_id, group_id, thread_id, memo, create, trigger, time_zone) VALUES {values};"
    )
