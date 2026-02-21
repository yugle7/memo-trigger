import os
import db
import hmac
import hashlib
import json

import dotenv

dotenv.load_dotenv()
TG_BOT_TOKEN = os.getenv("TG_BOT_TOKEN")


def handler(event, context):
    params = event["queryStringParameters"]

    # secret_key = hmac.new(
    #     key=b'WebAppData',
    #     msg=TG_BOT_TOKEN.encode(),
    #     digestmod=hashlib.sha256
    # ).digest()

    # calculated_hash = hmac.new(
    #     key=secret_key,
    #     msg=params['checkDataString'].encode(),
    #     digestmod=hashlib.sha256
    # ).hexdigest()

    # if calculated_hash != params['hash']:
    #     return {'statusCode': 200, 'body': []}

    user_id = params["user_id"]

    if "forms" in params:
        forms = json.loads(params["forms"])
        db.save_data(user_id, forms)
        return {"statusCode": 200, "body": "ok"}

    return {"statusCode": 200, "body": db.load_data(user_id)}
