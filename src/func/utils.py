import numpy as np
from bson import ObjectId
import json
import pandas as pd
import datetime


def dfToJson(df):
    health_idx = df.index
    health_key = df.keys()
    jsondata = {}
    for idx in health_idx:
        obj = {}
        for key in health_key:
            nested = {}
            if (type(key) is tuple):
                if (len(key) == 2):
                    if str(key[0]) in obj:
                        obj[str(key[0])].update(
                            {str(key[1]): df.at[idx, key]})
                    else:
                        nested = {str(key[1]): df.at[idx, key]}
                        obj.update({str(key[0]): nested})
            else:
                obj.update({str(key): df.at[idx, key]})

        jsondata.update({str(idx): obj})

    return jsondata


def correct_encoding(dictionary):
    """Correct the encoding of python dictionaries so they can be encoded to mongodb
    inputs
    -------
    dictionary : dictionary instance to add as document
    output
    -------
    new : new dictionary with (hopefully) corrected encodings"""

    new = {}
    for key1, val1 in dictionary.items():
        # Nested dictionaries
        if isinstance(val1, dict):
            val1 = correct_encoding(val1)

        if isinstance(val1, list):
            for i, item in enumerate(val1):
                val1[i] = correct_encoding(item)

        if isinstance(val1, np.bool_):
            val1 = bool(val1)

        if isinstance(val1, np.int64):
            val1 = int(val1)

        if isinstance(val1, np.float64):
            val1 = float(val1)

        # if isinstance(val1, unicode):
        #     for i in ['utf-8', 'iso-8859-1']:
        #         try:
        #             val1 = val1.encode(i)
        #         except (UnicodeEncodeError, UnicodeDecodeError):
        key1 = key1.replace('.', '-')
        new[key1] = val1

    return new


def convert_value(x):
    if isinstance(x, str):
        return x
    if isinstance(x, dict):
        return str(x)
    if isinstance(x, ObjectId):
        return str(x)
    if isinstance(x, int):
        return str(x)
    if isinstance(x, datetime.datetime):
        return x.isoformat()

    return str(x)


def duplicate_value(x: pd.Series):
    x = x.apply(convert_value)
    s = x.value_counts()
    count = 0
    for val in s.iteritems():
        if val[1] > 1:
            count = count + 1
    return count


def default(self, o):
    if isinstance(o, ObjectId):
        return str(o)
    return json.JSONEncoder.default(self, o)


def calcAge(birth_date, end_date):
    return end_date.year - birth_date.year - ((end_date.month, end_date.day) < (birth_date.month, birth_date.day))


def getStartDateTimeOfDay(ts):
    startDay = datetime.datetime(
        year=ts.year, month=ts.month, day=ts.day, hour=0, minute=0, second=0, microsecond=0)
    toLocalDay = datetime.datetime(
        year=ts.year, month=ts.month, day=ts.day, hour=16, minute=59, second=59, microsecond=999)

    if ts >= startDay and ts <= toLocalDay:
        # date of next day 00:00:00 - 16:59:59
        # day = ts.day - 1
        # print('date ===> ', day, ts)
        return datetime.datetime(year=ts.year, month=ts.month, day=ts.day, hour=17, minute=0, second=0, microsecond=0) - datetime.timedelta(days=1)
    else:
        return datetime.datetime(year=ts.year, month=ts.month, day=ts.day, hour=17, minute=0, second=0, microsecond=0)


def getTimeCluster(ts: datetime.datetime):
    return "{}:{}".format(ts.hour, ts.minute)


def generateCluster(ts: datetime.datetime = ..., period: int = ...):
    to = ts
    to = to + datetime.timedelta(days=1)
    clusters = list()
    currentDate = ts
    while currentDate < to:
        currentDate = currentDate + datetime.timedelta(milliseconds=period)
        cluster = {
            't': getTimeCluster(currentDate),
            # 'values': None,
        }
        clusters.append(cluster)
    return clusters


def getIndexClusters(clusters: list, key: str, value):
    res = None
    index = 0
    for sub in clusters:
        if sub[key] == value:
            res = sub
            break
        index += 1

    if res is None:
        index = -1
    return [index, res]

# def getIndexClusterByTime(clusters: list, ts: datetime.datetime):


#   getIndexClusterByTime(period: number, clusters: any[], ts: Date): number {
#     /* period (ms) */
#     const minutesRef = period / 60 / 1000;
#     const minutes = minutesRef - (ts.getMinutes() % minutesRef);
#     const atDate = new Date(new Date(ts).setMinutes(ts.getMinutes() + minutes, 0, 0));
#     const time = this.getTimeCluster(atDate);
#     const atIndex = clusters.findIndex(cluster => {
#       return cluster.t === time;
#     });

#     return atIndex;
#   }
