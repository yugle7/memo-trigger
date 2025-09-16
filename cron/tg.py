import os

import requests

import dotenv
import re

dotenv.load_dotenv()

TG_BOT_TOKEN = os.getenv('TG_BOT_TOKEN')
URL = 'https://api.telegram.org'


def escape(text):
    return re.sub(r'([:_~*\[\]()>#+-={}|.!])', r'\\\1', text)


def send_message(chat_id, text):
    url = f'{URL}/bot{TG_BOT_TOKEN}/sendMessage'
    data = {'chat_id': chat_id, 'text': escape(text), 'parse_mode': 'MarkdownV2'}

    res = requests.post(url, json=data)
    if not res.ok:
        return None
    res = res.json().get('result')
    return res and res.get('message_id')

