import requests
from datetime import date, datetime
from settings.configuration import Configuration
import json
import numpy as np
import func.utils as utils
import traceback
import logging


def createNotificationTask(method: str, sendTime: datetime, receiverIds: list, title: str, body: str, noti_type: int, data: dict):
    try:
        config = Configuration().getConfig()
        url = config.get('END_POINT', 'ANALYTIC_ENGINE') + '/notification-task'
        headers = {
            'X-API-ID': config.get('KEY', 'ANALYTIC_API_ID'),
            'X-API-KEY': config.get('KEY', 'ANALYTIC_API_KEY'),
            'Content-Type': 'application/json',
            'accept': '*/*'
        }

        payload = {
            'method': method,
            'sendTime':  sendTime.isoformat() + '.000Z',
            'receiverIds': receiverIds,
            'title': title + '',
            'body': body + '',
            'type': noti_type,
            'data': data
        }

        print('headers =>', headers)
        print('body =>', payload)
        jsonData = json.dumps(payload, cls=NpEncoder)
        print('json data  =>>', jsonData)
        r = requests.post(url, headers=headers, data=jsonData)
        print('status code =>', r.status_code)
        res = r.json()
        print('res =>', res)
        return res
    except Exception as e:
        logging.error('Thread %s: Error => {}'.format(
            e), constant.HEALTH_RECORD_PRE_PROCESS_NAME)
        traceback.print_exc()
        tb = traceback.format_exc()
        return tb


class NpEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        else:
            return super(NpEncoder, self).default(obj)
