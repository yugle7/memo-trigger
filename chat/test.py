from index import handler
from utils import get_triggers, get_request, get_requests, get_text


def test_get_triggers():
    memo, question = get_request('Скоро концерт 2025-10-01 в 20')
    words = get_text(question).split()
    print(memo, get_triggers(words))

    print(get_request('Скоро день рождения Глеба 20 октября 12:00'))
    for memo, question in get_requests('''
            День рождения Глеба 29 октября 12:00
            вернуть долг 20 октября 2025 10:00
            у тебя урок завтра пятница 19:00
            через неделю день рождения Ии 19 мая 13:00
        '''):
        print(memo, '-', question)


if __name__ == '__main__':
    handler({'body': open('body.json').read()})
