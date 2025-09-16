import db
import tg
from time import time


def create_memo(cron):
    tg.send_message(cron['group_id'], cron['memo'])
    cron['create'] = int(time())
    db.update_next(cron)


def handler(event=None, context=None):
    crons = db.read_crons()
    for c in crons:
        create_memo(c)

    return {'statusCode': 200, 'body': 'ok'}
