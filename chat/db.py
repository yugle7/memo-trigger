import ydb.iam

import os
import json

import dotenv

from utils import get_next, get_id

dotenv.load_dotenv()

driver = ydb.Driver(
    endpoint=os.getenv('YDB_ENDPOINT'),
    database=os.getenv('YDB_DATABASE'),
    credentials=ydb.AuthTokenCredentials(os.getenv('IAM_TOKEN'))
    # credentials=ydb.iam.MetadataUrlCredentials()
)

driver.wait(fail_fast=True, timeout=10)

pool = ydb.SessionPool(driver)

settings = ydb \
    .BaseRequestSettings() \
    .with_timeout(10) \
    .with_operation_timeout(8)


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


def load_crons(group_id):
    crons = execute(f'SELECT * FROM crons WHERE group_id={group_id} and create>=0;')

    for c in crons:
        c['triggers'] = json.loads(c['triggers'])

    return crons


def save_answer_id(chat_id, message_id, answer_id):
    id = get_id(chat_id, message_id)
    execute(f'INSERT INTO messages (id, answer_id) VALUES ({id}, {answer_id or "NULL"});')


def edit_answer_id(chat_id, message_id, answer_id):
    id = get_id(chat_id, message_id)
    execute(f'UPDATE messages SET answer_id={answer_id or "NULL"} WHERE id={id};')


def get_answer_id(chat_id, message_id):
    id = get_id(chat_id, message_id)
    res = execute(f'SELECT answer_id FROM messages WHERE id={id};')
    return res[0].get('answer_id') if res else None


def get_cron(id):
    res = execute(f'SELECT * FROM crons WHERE id={id};')
    if not res:
        return None
    cron = res[0]
    cron['triggers'] = json.loads(cron['triggers'])
    return cron


def get_user(id):
    res = execute(f'SELECT * FROM users WHERE id={id};')
    return res and res[0]


def create_user(id):
    res = execute(f'INSERT INTO users (id, time_zone) VALUES ({id}, 3) RETURNING id;')
    if not res:
        reset_user(id)
    return res


def set_time_zone(user_id, time_zone):
    execute(f'UPDATE users SET time_zone={time_zone} WHERE id={user_id};')


def set_where(user_id, group_id, thread_id):
    execute(f'UPDATE users SET cron_id=NULL, group_id={group_id}, thread_id={thread_id or "NULL"} WHERE id={user_id};')


def reset_where(user):
    id = user['id']
    user['group_id'] = user['thread_id'] = None
    execute(f'UPDATE users SET cron_id=NULL, group_id=NULL, thread_id=NULL WHERE id={id};')


def reset_user(id):
    execute(f'UPDATE users SET group_id=NULL WHERE id={id};')


def change_cron(cron):
    id = cron['id']
    triggers = cron['triggers']
    memo = cron['memo']

    cron['create'] = create = min(get_next(t) for t in triggers) if triggers else 0

    execute(f"UPDATE crons SET memo='{memo}', create={create}, triggers='{json.dumps(triggers)}' WHERE id={id};")


def create_cron(cron):
    id = cron['id']
    triggers = cron['triggers']
    memo = cron['memo']
    group_id = cron['group_id']
    thread_id = cron['thread_id']
    question_id = cron['question_id']

    cron['create'] = create = min(get_next(t) for t in triggers) if triggers else 0

    values = f"({id}, {group_id}, {thread_id}, '{memo}', {create}, '{json.dumps(triggers)}', {question_id})"
    execute(f"INSERT INTO crons (id, group_id, thread_id, memo, create, triggers, question_id) VALUES {values};")


def resume_cron(cron, user):
    id = cron['id']
    triggers = cron['triggers']

    cron['create'] = create = min(get_next(t) for t in triggers) if triggers else 0

    execute(f"UPDATE crons SET create={create} WHERE id={id};")


def stop_cron(cron):
    id = cron['id']
    cron['create'] = 0
    execute(f"UPDATE crons SET create=0 WHERE id={id};")


def delete_crons(ids):
    execute(f'DELETE FROM crons WHERE id IN ({",".join(map(str, ids))});')


def delete_cron(id):
    execute(f"DELETE FROM crons WHERE id={id};")


def stop_crons(group_id):
    execute(f"UPDATE crons SET create=0 WHERE group_id={group_id};")


def remove_crons(group_id):
    execute(f"DELETE FROM crons WHERE group_id={group_id};")


def get_cron_ids(question_id):
    res = execute(f'SELECT id FROM crons WHERE question_id={question_id};')
    return {c['id'] for c in res}
