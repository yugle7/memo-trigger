from datetime import datetime, timedelta
from croniter import croniter


def safe(func):
    def wrapper(*args):
        try:
            return func(*args)
        except Exception as e:
            print(f"{func.__name__}: {e}")
            return None

    return wrapper


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
