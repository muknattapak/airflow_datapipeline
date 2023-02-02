import pandas as pd
import datetime
import numpy as np
from datetime import datetime, timedelta, date
from bson import ObjectId


def check_dateOutOfBound(values):
    if pd.notna(values):
        if (pd.Timestamp.max < values) | (values < pd.Timestamp.min):
            return pd.to_datetime(pd.to_datetime(values.replace(year=(values.year-543)), errors='coerce').isoformat(sep=' ', timespec='seconds'))
        else:
            return pd.to_datetime(pd.to_datetime(values, errors='coerce').isoformat(sep=' ', timespec='seconds'))
    else:
        return values


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


def new_nest_dict(m):
    for val in m.values():
        if isinstance(val, dict):
            yield from new_nest_dict(val)
        else:
            yield val


def is_latest_id(**kwargs):
    r = redis.Redis(host='airflow_redis_1', port=6379, db=1)

    client = MongoClient(
        "mongodb://airflow:98ZaEVYxq758h2tk@ddb.thailand-smartliving.com:38200/aidery-health")

    health_db = client['aidery-health']
    healthColl = health_db['health-records']
    latestId = r.get('latestOfHealthRecords')
    print('LatestId:', latestId)

    latest_record = pd.DataFrame(list(healthColl.find({'type': {'$in': [
                                 4, 6, 7, 8, 9, 10, 13, 14, 19, 21]}}, {'_id': 1}).sort('_id', -1).limit(1)))

    if latestId is not None:
        # have new data in mongo
        if latest_record['_id'][0] > ObjectId(latestId.decode()):
            return True  # read new data
        else:
            return False  # skip read data
    else:
        return True


def retrieve_nested_value(mapping, key_of_interest):
    mappings = [mapping]
    while mappings:
        mapping = mappings.pop()
        try:
            items = mapping.items()
        except AttributeError:
            # we didn't store a mapping earlier on so just skip that value
            continue

        for key, value in items:
            if key == key_of_interest:
                yield value
            else:
                # type of the value will be checked in the next loop
                mappings.append(value)


def cal_threshold(values, thrsh):
    key_l = ['abnormal', 'urgent', 'emergency']

    for item in key_l:
        # item = 'emergency'
        grp_level = 'none'
        for level_l in retrieve_nested_value(thrsh['low'], item):
            if ('min' in level_l) & ('max' in level_l):
                if (level_l['min'] <= values <= level_l['max']):
                    grp_level = item
            elif ('max' in level_l):
                if (level_l['max'] >= values):
                    grp_level = item

        for level_l in retrieve_nested_value(thrsh['high'], item):
            if ('min' in level_l) & ('max' in level_l):
                if (level_l['min'] <= values <= level_l['max']):
                    grp_level = item
            elif ('min' in level_l):
                if (level_l['min'] <= values):
                    grp_level = item
        if (grp_level != 'none'):
            break

    if (grp_level == 'none'):
        grp_level = 'normal'

    return grp_level


def utc_to_local(utc_dt):
    local = 'Asia/Bangkok'
    try:
        local_tz = pytz.timezone(local)
        local_dt = utc_dt.replace(tzinfo=pytz.utc).astimezone(local_tz)
        return local_tz.normalize(local_dt)  # .normalize might be unnecessary
    except:
        return pd.NaT


def convert_tz(data):
    data_con = pd.to_datetime(data.apply(
        lambda x: utc_to_local(pd.to_datetime(x, errors='coerce'))))
    data_con = pd.to_datetime(
        (data_con.datetime.strftime('%Y-%m-%d %H:%M:%S')))
    return data_con


def movecol(df, cols_to_move=[], ref_col='', place='After'):
    cols = df.columns.tolist()
    if place == 'After':
        seg1 = cols[:list(cols).index(ref_col) + 1]
        seg2 = cols_to_move
    if place == 'Before':
        seg1 = cols[:list(cols).index(ref_col)]
        seg2 = cols_to_move + [ref_col]

    seg1 = [i for i in seg1 if i not in seg2]
    seg3 = [i for i in cols if i not in seg1 + seg2]

    return (df[seg1 + seg2 + seg3])


def letter_lower(data_csv):
    data_csv.columns = [c.lower() for c in data_csv.columns]
    return data_csv
