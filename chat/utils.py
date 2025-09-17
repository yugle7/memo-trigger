from cityhash import CityHash64
from datetime import datetime, timedelta
import re

from const import *


def get_id(a, b):
    return CityHash64(str(a) + ' ' + str(b).lower().strip())


def to_answer(cron, user):
    memo = cron['memo']
    stopped = not cron['create']
    answers = [f'"{memo}" {"не показывается" if stopped else "показывается"}']

    if cron['create']:
        time_zone = timedelta(hours=user['time_zone'])
        create = datetime.fromtimestamp(cron['create']) + time_zone
        month = MONTH[create.month - 1]
        answers.append(f'— покажу {create.day} {month} {create.hour}:00')

    triggers = cron['triggers']
    if triggers:
        answers.append(f'— показываю {when(triggers, user)}')

    return '\n'.join(answers)


def when(triggers, user):
    answers = []
    for t in triggers:
        y = t.get('year')
        m = t.get('month')
        d = t.get('day')
        w = t.get('weekday')

        n = get_next(t)
        if not n:
            answers.append('никогда')
            continue

        time_zone = timedelta(hours=user['time_zone'])
        n = datetime.fromtimestamp(n) + time_zone

        if w is not None:
            answer = WEEKDAY[n.weekday()]

        elif y and m and d:
            answer = f'{n.day} {MONTH[n.month - 1]} {n.year}'
        else:
            if d:
                if m:
                    answer = f'ежегодно {n.day} {MONTH[n.month - 1]}'
                else:
                    answer = f'ежемесячно {n.day} числа'
            elif m:
                answer = f'ежедневно весь {MONTH[m - 1]}'
            else:
                answer = f'каждый день'

            if y:
                answer += f'в {y} году'

        answers.append(f'{answer} {n.hour:02}:00')

    return ', '.join(answers)


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


class Token:
    def __init__(self, k, v=None):
        self.k = k
        self.v = v


def stem(word):
    word = word.removesuffix('те')
    for end in ACTION_ENDS:
        if word.endswith(end):
            return word[:-len(end)]
    return word


def get_command(words):
    for w in words:
        a = ACTIONS.get(stem(w))
        if a in ('stop', 'resume', 'delete', 'show'):
            return a
    return 'change' if any(w.isdigit() for w in words) else ''


def get_text(text):
    text = text.lower().replace('ё', 'е').replace(',', '\n').replace(MENTION, '\n')
    text = re.sub(r':\d+', ' час', text)
    text = re.sub(r'[^0-9а-яn\n]', ' ', text)
    text = re.sub(r'(\d\d\d\d) (\d\d?) (\d\d?)', r'\1-\2-\3', text)
    text = re.sub(r'(\d\d?) (\d\d?) (\d\d\d\d)', r'\3-\2-\1', text)
    text = text.replace('полночь', '0 час')
    text = text.replace('полдень', '12 час')
    return text.replace('\n', ' | ')


class H:
    def __init__(self):
        self.q = []
        self.s = []
        self.h = None

    def add(self, q):
        h = 'hour' in q
        if self.h is not None and self.h != h:
            self.s.append(self.q)
            self.q = [q]
        else:
            self.q.append(q)
        self.h = h

    def get(self):
        while self.s:
            s = self.s.pop()
            if not self.q:
                self.q = s
                continue
            for y in s:
                for x in self.q:
                    y.update(x)
                    yield y
            self.q = []
        for q in self.q:
            if 'hour' in q:
                yield q


def get_triggers(words):
    tokens = get_tokens(words)

    if not tokens:
        return None

    h = H()
    k = ''.join(t.k for t in tokens)

    for m in re.finditer('w|nmN?|d|nh?', k):
        i, j = m.span()

        if m[0] in ('n', 'nh'):
            hour = tokens[i].v
            if hour >= 24:
                return []
            h.add({'hour': hour})
        elif m[0] == 'w':
            h.add({'weekday': tokens[i].v})
        elif m[0] == 'nmN':
            h.add({
                'day': tokens[i].v,
                'month': tokens[i + 1].v,
                'year': tokens[i + 2].v
            })
        elif m[0] == 'nm':
            h.add({
                'day': tokens[i].v,
                'month': tokens[i + 1].v,
            })
        elif m[0] == 'd':
            h.add({
                'day': tokens[i].v[0],
                'month': tokens[i].v[1],
                'year': tokens[i].v[2]
            })

    return list(h.get())


def get_tokens(words):
    tokens = []
    for i in range(len(words)):
        t = get_number(words, i) or get_action(words, i) or get_weekday(words, i) or get_month(words, i) or get_date(words, i) or get_void(words, i)
        if t is None:
            tokens.append(Token(' '))
        elif t.k != 'v':
            tokens.append(t)

    V = {t.v for t in tokens if t.k == 'A'}
    v = {t.v for t in tokens if t.k == 'a'}
    if V & v:
        return None

    return tokens


def get_date(words, i):
    m = re.fullmatch(r'(\d\d\d\d)-(\d\d?)-(\d\d?)', words[i])
    return m and Token('d', (int(m[3]), int(m[2]), int(m[1])))


def get_split(words, i):
    return Token('|') if words[i] == '|' else None


def get_void(words, i):
    return Token('v') if words[i] in VOIDS else None


def get_action(words, i):
    w = stem(words[i])
    if w not in ACTIONS:
        return None
    action = ACTIONS[w]
    if i > 0 and words[i - 1] in NOT:
        return Token('A', action)
    return Token('a', action)


def get_hour(words, i):
    w = words[i]
    if w.startswith('час') and len(w) <= 4:
        i += 1
        if i < len(words) and words[i] in ('дня', 'вечера'):
            return Token('h', 12)
        return Token('h', 0)
    return None


def get_number(words, i):
    w = words[i]
    if w.isdigit():
        number = int(w)
        i += 1
    elif w in TENS:
        number = TENS[w]
        i += 1
        if i < len(words) and words[i] in DIGITS:
            number += DIGITS[words[i]]
            words[i] = ''
    elif w in TEENS:
        number = TENS[w]
    else:
        return None
    return Token('N' if number > 31 else 'n', number)


def get_weekday(words, i):
    w = words[i]
    if w in WEEKDAY:
        weekday = WEEKDAY.index(w)
    else:
        for end in WEEKDAY_ENDS:
            w = w.removesuffix(end)
        if w in WEEKDAYS:
            weekday = WEEKDAYS.index(w)
        else:
            return None
    return Token('w', weekday)


def get_month(words, i):
    w = words[i]
    if w in MONTH:
        month = MONTH.index(w) + 1
    else:
        if w[-1] in MONTH_ENDS:
            w = w[:-1]
        if w in MONTHS:
            month = MONTHS.index(w) + 1
        else:
            return None
    return Token('m', month)


def is_month(word):
    return word in MONTH or word in MONTHS or (word[-1] in MONTH_ENDS and word[:-1] in MONTHS)


def is_number(word):
    return word in TEENS or word in TEENS or word in DIGITS


def is_time(word):
    return word.startswith('час') or word in ('утра', 'дня', 'вечера', 'ночи', 'полночь', 'полдень')


def is_weekday(word):
    if word in WEEKDAY:
        return True
    for end in WEEKDAY_ENDS:
        word = word.removesuffix(end)
    return word in WEEKDAYS


def is_action(word):
    return stem(word) in ACTIONS


def is_question(word):
    word = re.sub('[^а-я0-9!?]', '', word.lower().replace('ё', 'е'))
    if not word:
        return False
    return is_month(word) or word.isdigit() or is_number(word) or is_time(word) or is_weekday(word) or word in VOIDS or is_action(word)


def are_questions(words):
    n = len(words)
    i = 0
    while i < n and is_question(words[i]):
        i += 1
    j = 1
    while i + j < n and is_question(words[-j]):
        j += 1
    j -= 1
    return i, n - i - j, j


def get_request(text):
    words = text.split()
    i, k, j = are_questions(words)

    if i == j == 0:
        return text, ''

    if i > j:
        memo, question = ' '.join(words[i:]), ' '.join(words[:i])
    else:
        memo, question = ' '.join(words[:-j]), ' '.join(words[-j:])

    memo = memo.removesuffix(',').removesuffix(':')
    return memo, question


def get_requests(text):
    texts = text.split('\n')

    counts = {q: 0 for q in 'ikj'}
    requests = {q: [] for q in 'ikj'}

    memos = []
    for text in texts:
        text = text.strip()
        if not text:
            continue

        words = text.split()
        i, k, j = are_questions(words)

        requests['i'].append((' '.join(words[i:]), ' '.join(words[:i])))
        requests['j'].append((' '.join(words[:-j]), ' '.join(words[-j:])))

        if i + j <= k:
            memos.append(text)
        else:
            for memo in memos:
                requests['k'].append((memo, text))
            memos = []
        if i == j == 0 or k == 0:
            counts['k'] += 1
        elif i > j:
            counts['i'] += 1
        else:
            counts['j'] += 1

    if counts['k'] >= counts['i'] + counts['j']:
        return requests['k']
    if counts['i'] > counts['j']:
        return requests['i']
    return requests['j']
