import json
import requests
import datetime

DINGTALK_WEBHOOK = 'https://oapi.dingtalk.com/robot/send?access_token' \
                   '=7e2a177951a4fd88d3f10ede0952f3d6839e297cfc3976176c26f94b73663b12 '


def send_to_dingtalk(msg):
    header = {
        "Content-Type": "application/json",
        "charset": "utf-8"
    }
    content = {
        'msgtype': 'text',
        'text': {
            'content': msg
        }
    }
    json_data = json.dumps(content)
    requests.post(DINGTALK_WEBHOOK, data=json_data, headers=header)


class Stock(object):
    def __init__(self, code, name):
        self.code = code
        self.name = name

    def set_ipo_date(self, date):
        self.ipo_date_str = date
        self.ipo_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        today = datetime.date.today()
        self.has_ipo_days = (today - self.ipo_date).days