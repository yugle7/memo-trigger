import json

import db
import tg

BOT_NAME = "memotriggerbot"


def handle(body):
    message = body.get("message")
    if not message:
        return "нет сообщения"

    user_id = message["from"]["id"]
    chat_id = message["chat"]["id"]

    text = message.get("text")

    if chat_id != user_id:
        group_id = chat_id

        if not text or not text.startswith("/"):
            return "не команда"

        if "@" in text and text.endswith("@memotriggerbot"):
            return "не ко мне"

        if not tg.is_admin(user_id, group_id):
            return "не админ"

        thread_id = message.get("message_thread_id")

        if not db.get_user(user_id):
            tg.show_message(
                group_id, thread_id, "Сначала заведите личную переписку со мной"
            )
            return "нет чата со мной"

        group = message["chat"]["title"]
        reply = message.get("reply_to_message")
        if thread_id and reply:
            thread = reply["forum_topic_created"]["name"]
        else:
            thread = None

        if "/attach" in text:
            db.attach_chat(user_id, group_id, group, thread_id, thread)
            return "связал"

        if "/detach" in text:
            db.detach_chat(user_id, group_id, thread_id)
            return "отвязал"

        return "не понял"

    elif text == "/start":
        if db.create_user(user_id):
            answer = "\n\n".join(
                [
                    "Приятно познакомиться!",
                    "Я умею создавать напоминания здесь или в другом чате",
                    "В другом чате отправьте мне команду /attach",
                    "(вы должны быть там администратором)",
                ]
            )
        else:
            answer = "Если хотите создавать напоминания в другом чате,\nдобавьте меня туда и отправьте команду /attach"

        tg.send_message(user_id, answer)
        return "старт"

    return "тоже успех"


def handler(event, context=None):
    body = json.loads(event["body"])

    try:
        return {"statusCode": 200, "body": handle(body)}

    except Exception as e:
        print(e)

    return {"statusCode": 200, "body": "fail"}
