from datetime import datetime, timedelta
from uuid import uuid4
from croniter import croniter

WEEKDAYS = {
    "пн": 0,
    "понедельник": 0,
    "понедельникам": 0,
    "вт": 1,
    "вторник": 1,
    "вторникам": 1,
    "ср": 2,
    "среда": 2,
    "средам": 2,
    "среду": 2,
    "чт": 3,
    "четверг": 3,
    "четвергам": 3,
    "пт": 4,
    "пятница": 4,
    "пятницам": 4,
    "пятницу": 4,
    "сб": 5,
    "суббота": 5,
    "субботу": 5,
    "субботам": 5,
    "вс": 6,
    "воскресенье": 6,
    "воскресеньям": 6,
}


def get_trigger(when):
    words = when.lower().replace(":", " ").split()
    trigger = {}

    for word in words:
        if word in WEEKDAYS:
            trigger["weekday"] = WEEKDAYS[word]
            break

    for word in words:
        if word.isdigit():
            hour = int(word)
            if 0 <= hour <= 23:
                trigger["hour"] = hour
                break

    return trigger


def get_when(trigger, time_zone):
    if "hour" not in trigger:
        return 0
    c = f'* {trigger["hour"]} {trigger.get("day", "*")} {trigger.get("month", "*")} {trigger.get("weekday", "*")}'
    time_zone = timedelta(hours=time_zone)
    s = datetime.now() + time_zone
    if "year" in trigger and trigger["year"] > s.year:
        s = datetime(trigger["year"], 1, 1) - timedelta(minutes=1)
    t = croniter(c, s).get_next(datetime)
    if "year" in trigger and t.year != trigger["year"]:
        return 0

    return int((t - time_zone).timestamp())


def get_random_id():
    return str(uuid4().int % (1 << 64))


def get_form(form):
    form["id"] = form.get("id") or get_random_id()
    form["time_zone"] = int(form["time_zone"])
    return form


def get_cron(form, user_id):
    if not form["chat"]:
        group_id = user_id
        thread_id = "NULL"
    else:
        group_id, thread_id = form["chat"].split()
        group_id = int(group_id)
        if thread_id != "null":
            thread_id = int(thread_id)

    trigger = get_trigger(form["when"])
    if form.get("day"):
        trigger["day"] = int(form["day"])
    if form.get("month"):
        trigger["month"] = int(form["month"])
    if form.get("year"):
        trigger["year"] = int(form["year"])

    return {
        "id": int(form["id"]),
        "group_id": group_id,
        "thread_id": thread_id,
        "memo": form["what"],
        "create": 0 if form.get("stop") else get_when(trigger, form["time_zone"]),
        "trigger": trigger,
        "time_zone": form["time_zone"],
    }
