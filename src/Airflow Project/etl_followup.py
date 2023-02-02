import constant
from bson import ObjectId
from func.database_connect import DatabaseConnect
import pytz
import pandas as pd
from pandas.io.json import json_normalize
import json
import sys
from pymongo import MongoClient
from datetime import datetime, timedelta

sys.path.insert(
    0, '/Users/thawaree/Documents/GitHub/zg-analytics-engine-python/src')

domains = pd.read_json(
    '/Users/thawaree/Desktop/ZG Dataset/wesafe_domain/official-domain-hici.json')


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
    data_con = pd.to_datetime((data_con.dt.strftime('%Y-%m-%d %H:%M:%S')))
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

    return(df[seg1 + seg2 + seg3])


# ======================= 1. Create connection =======================
connection = DatabaseConnect()

# ------------ connect to collection ------------
authDb = connection.getAuthDB()
healthDb = connection.getHealthDB()


# ---------------------------------load domains---------------------------------
# load domains
domaColl = authDb.get_collection(constant.AUTH_DOMAIN_COLL)
# result = domaColl.find({'meta': {'isolation': 1}},{'_id', 'crt','name', 'domainName', 'type', 'status', 'parentId','meta'})
result = domaColl.find()
list_cur = list(result)
domain_df = pd.DataFrame(list_cur)
domain_df = domain_df[['_id', 'crt', 'name',
                       'domainName', 'type', 'status', 'parentId', 'meta']]
domain_df = domain_df[domain_df['meta'].notna()]
domain_df = pd.concat([domain_df.drop('meta', axis=1),
                       domain_df['meta'].apply(pd.Series)], axis=1)
# domain_df.to_csv('/Users/thawaree/Desktop/ZG Dataset/wesafe_domain/wesafe_domains_20220821.csv', index=False)


# --------------------------------load user----------------------------------
doma = domain_df['_id'].copy().reset_index(drop=True)
# load user
userColl = authDb.get_collection(constant.AUTH_USER_COLL)
# res_count = userColl.find({'domainId': doma}).count()
user_df = pd.DataFrame()
for i in range(0, len(doma), 1):
    # result = userColl.find(
    #     {'domainId': ObjectId(doma[i]), 'role': {'$in': [1]}, 'status': {'$gt': -1}}, {'_id', 'role', 'domainId', 'crt'})
    result = userColl.find({'_id': ObjectId('62248f2652b967002a5ff687')}, {
                           '_id', 'role', 'domainId', 'crt'})
    list_cur = list(result)
    temp = pd.DataFrame(list_cur)
    user_df = temp
    # user_df = user_df[['_id', 'domainId', 'gender',
    #                    'birthday', 'weight', 'height', 'swabDate']]
    # user_df = user_df.append(temp)


# ----------------------------load survey--------------------------------------
surveyColl = healthDb.get_collection(constant.SURVEY_ANSWER_COLL)
# load survey
user_str_list = user_df['_id'].copy()
surv_df = pd.DataFrame()
for i in range(0, len(user_str_list), 1):
    result = surveyColl.find(
        {'formId': ObjectId('60e72f655f16d200112764c5'), 'userId': '62248f2652b967002a5ff687'})  # str(user_str_list[i])
    # result = healthColl.find().sort("to", -1).limit(3000) # find all data from lastest data
    list_cur = list(result)
    temp = pd.DataFrame(list_cur)
    surv_df = temp[['_id', 'userId', 'domainId', 'response', 'ts', 'crt']]
    # surv_df = surv_df.append(temp)
# surv_df.shape
temp_surv = pd.concat([surv_df.drop('response', axis=1),
                       surv_df['response'].apply(pd.Series)], axis=1)
surveys = pd.concat([temp_surv.drop('Symptoms', axis=1),
                     temp_surv['Symptoms'].apply(pd.Series)], axis=1)
# surveys.shape
surveys.rename(columns={'healthtype-8': 'bodyTemp', 'healthtype-13': 'oxygen', 'healthtype-7': 'pulse', 'healthtype-13-1': 'oxygenPost',
                        'healthtype-7-1': 'pulsePost'}, inplace=True)
surveys = surveys.sort_values(by='ts', ascending=True).reset_index(drop=True)
surveys['ts_loc'] = convert_tz(surveys['ts'])
surveys = movecol(surveys,
                  cols_to_move=['ts_loc'],
                  ref_col='ts',
                  place='After')

# -----------------------------load personal-------------------------------------
# load personal
personalColl = authDb.get_collection(constant.AUTH_PERSONAL_RECORDS_COLL)
user_list = user_df['_id'].copy()
badgecol_df = pd.DataFrame()
for i in range(0, len(user_list), 1):
    result = personalColl.find(
        {'userId': ObjectId('62248f2652b967002a5ff687'), 'tags': ['badge-status-history']})  # user_list[i]
    list_cur = list(result)
    temp = pd.DataFrame(list_cur)
    badgecol_df = temp[['_id', 'userId', 'data', 'crt']]
    # badgecol_df = badgecol_df.append(temp)
# badgecol_df.shape

#  Transforms badge color
df = pd.concat([badgecol_df.drop('data', axis=1),
                badgecol_df['data'].apply(pd.Series)],
               axis=1).explode(['dates']).apply(pd.Series).reset_index(drop=True)
bdgColor = pd.concat([df.drop('dates', axis=1),
                      df['dates'].apply(pd.Series)], axis=1)
bdgColor['date_loc'] = convert_tz(bdgColor['date'])
bdgColor = movecol(bdgColor,
                   cols_to_move=['date_loc'],
                   ref_col='date',
                   place='After')


# =============== Transform ==================
surveys['dateColor'] = pd.to_datetime(surveys['ts'].dt.date)
bdgColor['dateColor'] = pd.to_datetime(
    pd.to_datetime(bdgColor['date']).dt.date)

followUp = pd.merge(
    surveys, bdgColor[['dateColor', 'status']], how='left', on='dateColor')

# add user crt
followUp.loc[0:len(followUp), 'reg_crt'] = user_df['crt'][0]

# cal treat day
# followUp['treatDay'] = (followUp.iloc[-1]['ts'] - followUp.iloc[0]['reg_crt']).days
today = pd.to_datetime(datetime.utcnow().date())
followUp['treatDay'] = (today - followUp.iloc[0]['reg_crt']).days

# fill nan badgecolor
followUp['status'] = followUp.groupby([pd.Grouper(key='ts', freq='D'), '_id'])[
    'status'].mean().ffill().reset_index()['status']

# add discharge status
if any(x == 7 for x in followUp['status']) | all(followUp['treatDay'] > 15):
    followUp['dc_status'] = 'D/C'
else:
    followUp['dc_status'] = 'Inpatient'

# add discharge date
if any(x == 7 for x in followUp['status']):
    followUp['dc_date'] = followUp[followUp.groupby('_id')['status'].transform(
        lambda s:  7 in s.values)].reset_index(drop=True).loc[0, 'dateColor']
elif all(followUp['treatDay'] > 15):
    followUp['dc_date'] = followUp.iloc[0]['reg_crt'] + pd.DateOffset(days=15)
else:
    followUp['dc_date'] = pd.NaT
