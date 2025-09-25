import json
import re
from datetime import datetime

import tg
import db

from utils import get_id, to_answer, get_command, get_triggers, get_request, get_requests, get_text


def handler(event, context=None):
    body = json.loads(event['body'])
    print('body:', body)

    try:
        answer = handle(body)

    except Exception as err:
        print(err)
        answer = 'фатальная ошибка'

    return {'statusCode': 200, 'body': answer}


def send_answer(text, chat_id, message_id, edited):
    print(text)

    answer_id = edited and db.get_answer_id(chat_id, message_id)

    if answer_id:
        if not tg.edit_message(chat_id, answer_id, text):
            tg.delete_message(chat_id, answer_id)
            answer_id = tg.send_message(chat_id, text)
            db.edit_answer_id(chat_id, message_id, answer_id)
    else:
        answer_id = tg.send_message(chat_id, text)
        db.save_answer_id(chat_id, message_id, answer_id)


def create_memo(cron):
    tg.show_message(cron['group_id'], cron['thread_id'], cron['memo'])


def handle(body):
    message = body.get('message') or body.get('edited_message')
    if not message:
        return 'нет сообщения'

    edited = 'edited_message' in body

    user_id = message['from']['id']
    chat_id = message['chat']['id']
    message_id = message['message_id']
    text = message.get('text')

    try:
        if chat_id != user_id:
            if not text or '@' not in text:
                return 'не ко мне'

            thread_id = message.get('message_thread_id')

            if not db.get_user(user_id):
                tg.show_message(chat_id, thread_id, 'Сначала заведите личную переписку со мной')
                return 'не узнал'

            title = message['chat']['title']
            reply = message.get('reply_to_message')
            if thread_id and reply:
                topic = reply['forum_topic_created']['name']
                title = f'{title} ({topic})'

            answer = mention_in_text(user_id, chat_id, thread_id, title)

        elif not text:
            return 'нет текста'

        elif text == '/start':
            if db.create_user(user_id):
                answer = 'Отлично! Рад вас видеть!\n\nПо умолчанию напоминания будут показываться здесь. Но если хотите показывать напоминания в какой-нибудь группе, то добавьте меня в нее и свяжите меня с ней, отправив туда сообщение /start.\n\nПо умолчанию время московское. Если захотите изменить его, то отправьте мне сколько сейчас времени у вас в формате 12:34'
            else:
                answer = 'Привет! О чём вам напомнить?\n\nСейчас вы создаёте личные напоминания, которые будут показываться здесь. Но вы можете привязать меня к какой-нибудь группе, если их нужно показывать там.'

        elif re.fullmatch(r'\d+:\d+', text):
            answer = set_time_zone(user_id, text)

        else:
            user = db.get_user(user_id)
            if not user:
                return 'нет такого пользователя'

            get_where(user)

            question_id = get_id(chat_id, message_id)
            reply = message.get('reply_to_message')
            if reply:
                memo = reply.get('text').strip()
                if not memo or '\n' in memo:
                    answer = 'Вы на что-то ответили, но не на напоминание'
                else:
                    answer = execute(user, memo, text, question_id)
            else:
                memos = set()

                if '\n' in text:
                    requests = get_requests(text)
                    answers = []
                    for memo, text in requests:
                        memos.add(memo)
                        answers.append(execute(user, memo, text, question_id))
                    answer = '\n\n'.join(answers)
                else:
                    memo, question = get_request(text)
                    memos.add(memo)
                    if memo:
                        answer = execute(user, memo, question, question_id)
                    else:
                        answer = executes(user, text)

                if edited:
                    where_id = get_id(user['group_id'] or user['id'], user['thread_id'])
                    cron_ids = {get_id(where_id, memo) for memo in memos}
                    cron_ids = db.get_cron_ids(question_id) - cron_ids

                    if len(cron_ids) == 1:
                        db.delete_cron(cron_ids.pop())

                    elif len(cron_ids) > 1:
                        db.delete_crons(cron_ids)

    except Exception as err:
        print(err)
        answer = 'Что-то пошло не так'

    if not answer:
        return 'тоже успех'

    send_answer(answer, user_id, message_id, edited)
    return answer


def mention_in_text(user_id, group_id, thread_id, title):
    if not tg.is_admin(user_id, group_id):
        return f'У вас не хватает прав, чтобы создавать напоминания в группе "{title}", станьте сначала в ней администратором'

    db.set_where(user_id, group_id, thread_id)
    return f'Теперь вы можете здесь создавать напоминания и задавать время, когда их показывать в группе "{title}"'


def set_time_zone(user_id, text):
    h, m = map(int, text.split(':'))
    if not (0 <= h < 24 and 0 <= m < 60):
        return 'Я не смог понять какое у вас время, ожидаю формат 12:34'

    now = datetime.now()
    time_zone = (24 + h + m / 60 - now.hour - now.minute / 60) % 24
    time_zone = int(time_zone + 0.5)
    db.set_time_zone(user_id, time_zone)

    return f'Установлен часовой пояс UTC+{time_zone}'


def get_where(user):
    if user['group_id'] and not tg.is_admin(user['id'], user['group_id']):
        db.reset_where(user)


def get_cron(user, memo, question_id):
    group_id = user['group_id'] or user['id']
    where_id = get_id(group_id, user['thread_id'])

    cron_id = get_id(where_id, memo)
    cron = db.get_cron(cron_id)
    if cron:
        cron['memo'] = memo
        return cron

    return {
        'id': cron_id,
        'memo': memo,
        'group_id': group_id,
        'thread_id': thread_id,
        'triggers': [],
        'question_id': question_id
    }


def execute(user, memo, text, question_id):
    words = get_text(text).split()
    command = get_command(words)

    cron = get_cron(user, memo, question_id)

    if command == 'resume':
        if not cron['triggers']:
            return f'У напоминания "{memo}" еще не задано время создания, чтобы его возобновлять'

        if cron.get('create'):
            return f'Напоминание "{memo}" уже и так показывается'

        db.resume_cron(cron, user)
        return to_answer(cron, user)

    if command == 'stop':
        if not cron.get('create'):
            return f'Напоминание "{memo}" и так не показывалось'

        db.stop_cron(cron)
        return to_answer(cron, user)

    if command == 'delete':
        if 'create' not in cron:
            return f'Напоминание "{memo}" и так было удалено'

        db.delete_cron(cron['id'])
        return f'Напоминание "{memo}" удалено'

    if command == 'create':
        return create_memo(cron)

    if command == 'change':
        triggers = get_triggers(words)
        if not triggers:
            return f'Не понял как нужно поменять время показа напоминания "{memo}"'

        for t in triggers:
            t['time_zone'] = user['time_zone']
        cron['triggers'] = triggers

        if 'create' in cron:
            db.change_cron(cron)
        else:
            db.create_cron(cron)

        return to_answer(cron, user)

    if command == 'show':
        return to_answer(cron, user)

    return f'Что мне сделать с напоминанием "{memo}"?'


def executes(user, text):
    words = get_text(text).split()
    command = get_command(words)

    group_id = user['group_id'] or user['id']
    crons = db.load_crons(group_id)

    def where():
        if user['group_id']:
            title = tg.get_chat(user['group_id'])['title']
            return f'В группе {title}'
        return 'У вас'

    if not crons:
        return f'{where()} еще нет напоминаний'

    if command == 'show':
        return '\n\n'.join(to_answer(cron, user) for cron in crons)

    if command == 'delete':
        db.remove_crons(group_id)
        return '\n\n'.join(f'{cron["memo"]} - удалено' for cron in crons)

    if command == 'stop':
        answers = []
        for cron in crons:
            if cron.get('create'):
                answers.append(to_answer(cron, user))

        if not answers:
            return f'{where()} напоминания и так не показываются'

        db.stop_crons(group_id)
        return '\n\n'.join(answers)

    if command == 'resume':
        answers = []
        for cron in crons:
            if not cron['triggers']:
                answers.append(f'"{cron["memo"]}" - не задано время создания')

            if cron['create'] == 0:
                db.resume_cron(cron, user)
                answers.append(to_answer(cron, user))

        return '\n\n'.join(answers)

    return 'Что мне сделать с напоминаниями?'
