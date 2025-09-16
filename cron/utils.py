from datetime import datetime, timedelta


def get_next(trigger):
    time_zone = timedelta(hours=trigger['time_zone'])
    now = datetime.now() + time_zone
    t = now.replace(hour=trigger['hour'], minute=0, second=0, microsecond=0)

    y = trigger.get('year')
    m = trigger.get('month')
    d = trigger.get('day')

    if y:
        t = t.replace(year=y, month=1, day=1)
    if m:
        t = t.replace(month=m, day=1)
    if d:
        t = t.replace(day=d)

    if 'weekday' in trigger:
        days = (trigger['weekday'] - t.weekday()) % 7
        if days:
            t += timedelta(days=days)
        elif t <= now:
            t += timedelta(days=7)

    elif t <= now:
        if y and m and d:
            return 0

        if d:
            if m:
                t = t.replace(year=t.year + 1)
            elif t.month == 12:
                t = t.replace(year=t.year + 1, month=1)
            else:
                t = t.replace(month=t.month + 1)
        else:
            t += timedelta(days=1)

    if m and t.month != m:
        return 0

    return int((t - time_zone).timestamp())
