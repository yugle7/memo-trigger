import ydb.iam

import os
import json
import dotenv

from cityhash import CityHash64


dotenv.load_dotenv()

driver = ydb.Driver(
    endpoint=os.getenv("YDB_ENDPOINT"),
    database=os.getenv("YDB_DATABASE"),
    # credentials=ydb.AuthTokenCredentials(os.getenv("IAM_TOKEN")),
    credentials=ydb.iam.MetadataUrlCredentials(),
)

driver.wait(fail_fast=True, timeout=10)

pool = ydb.SessionPool(driver)

settings = ydb.BaseRequestSettings().with_timeout(10).with_operation_timeout(8)


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


def attach_chat(user_id, group_id, group, thread_id, thread):
    id = CityHash64(f"{user_id} {group_id} {thread_id}")
    chat = {
        "group_id": group_id,
        "group": group,
        "thread_id": thread_id,
        "thread": thread,
    }
    execute(
        f"INSERT INTO chats (id, user_id, chat) VALUES ({id}, {user_id}, '{json.dumps(chat)}');"
    )


def detach_chat(user_id, group_id, thread_id):
    id = CityHash64(f"{user_id} {group_id} {thread_id}")
    execute(f"DELETE FROM chats WHERE id={id};")


def get_user(id):
    res = execute(f"SELECT * FROM users WHERE id={id};")
    return res and res[0]


def create_user(id):
    return execute(f"INSERT INTO users (id, time_zone) VALUES ({id}, 3) RETURNING id;")
