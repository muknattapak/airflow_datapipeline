from constant import EDailyReportCategory
import constant
import traceback
import logging
import pandas as pd
import numpy as np
from bson.objectid import ObjectId
import func.utils as utils
from datetime import date, datetime
import func.http_service as hs
import pytz


class HealthIndicator:
    def __init__(self, auth_db, health_db, report_db):
        self.auth_db = auth_db
        self.health_db = health_db
        self.report_db = report_db

    def process(self, data, start, end):
        try:
            #  data = 10 min
            for item in data:

                userId = item['_id']
                healths = item['healths']
                print('userId', userId)
                data_set = pd.DataFrame(healths)
                data_set = data_set.drop_duplicates(
                    subset=['provider', 'type', 'from', 'to', 'value'], keep='first')
                # get age user from birthday to health at date
                age = self.getUserAge(userId, healths[0]['to'])
                if age is None:
                    continue

                # calc indicator
                user_hr_zone = self.calcUserHeartRateZone(
                    userId, data_set, age)
                print('user_hr_zone', user_hr_zone)

                clust_steps = self.calcStepsCluster(userId, data_set)
                print('clust_steps', clust_steps)

                clust_hr = self.calcHeartRateCluster(
                    userId, data_set, user_hr_zone)
                print('clust_hr', clust_hr)

                hr_event = self.calcHeartRateMetrics(clust_hr, clust_steps)
                print('hr_event', hr_event)

                # get daily report
                ts = utils.getStartDateTimeOfDay(start)
                daily = self.getDailyReport(userId, ts)
                if daily is None:
                    self.createDailyReport(userId, ts)
                    daily = self.getDailyReport(userId, ts)

                # check clusters in undefined
                if 'clusters' not in daily:
                    daily['clusters'] = utils.generateCluster(
                        ts, 10 * 60 * 1000)

                # find cluster of index
                t = utils.getTimeCluster(start)
                # t = '17:30'
                [index, value] = utils.getIndexClusters(
                    daily['clusters'], 't', t)
                if index < 0:
                    print('t index === 0', t)
                    continue

                cluster = {
                    'steps': hr_event.at[0, 'steps'],
                    'hrMax': hr_event.at[0, 'hr_max'],
                    'hrOver': hr_event.at[0, 'hr_over'],
                    'hrZone': hr_event.at[0, 'zone'],
                    'hrLabel': hr_event.at[0, 'hr_label'],
                }
                print('cluster', cluster)

                daily['clusters'][index] = {
                    **daily['clusters'][index], **cluster}
                print(daily['clusters'][index])
                self.updateDailyReport(daily['_id'], daily['clusters'])

                # check time is old
                now = datetime.now()
                time_period = now - end
                time_period_in_s = time_period.total_seconds()
                print('time =>>', end, time_period_in_s)

                if time_period_in_s > 6 * 60 * 60:  # 6 hr
                    continue

                # sent notification
                if cluster['hrOver'] >= 50:
                    print('sent noti')
                    self.createNotification(start, end, userId, cluster)

            return False
        except Exception as e:
            logging.error('Thread %s: Error => {}'.format(
                e), constant.HEALTH_RECORD_PRE_PROCESS_NAME)
            traceback.print_exc()
            tb = traceback.format_exc()
            return tb

    def getUserAge(self, userId, end_date):
        user_col = self.auth_db[constant.AUTH_USER_COLL]
        query = {
            '_id': ObjectId(userId)
        }
        user = user_col.find_one(query, {'birthday': 1})
        print('user', user)
        if user is None:
            return None

        if user.get('birthday') is None:
            return None

        age = utils.calcAge(user['birthday'], end_date)
        print('age', user, age)
        return age

    def calcUserHeartRateZone(self, userid, data, age):
        if len(data) > 0:
            max_hr = 220 - age
            hr_zone = pd.DataFrame(
                {'uid': userid, 'zone': ['zone1', 'zone2', 'zone3', 'zone4', 'zone5'], 'label': ['very light', 'light', 'moderate', 'hard', 'maximum'],
                 'lowvar': [0.5, 0.6, 0.7, 0.8, 0.9], 'upvar': [0.6, 0.7, 0.8, 0.9, 1.0], 'lower': 0, 'upper': 0})
            for i in hr_zone.index:
                hr_zone.iloc[i, hr_zone.columns.get_loc('lower')] = (
                    max_hr * hr_zone['lowvar'].iloc[i]).round(decimals=0)
                hr_zone.iloc[i, hr_zone.columns.get_loc('upper')] = (
                    max_hr * hr_zone['upvar'].iloc[i]).round(decimals=0)
            temp = pd.DataFrame({'uid': [userid, userid, userid], 'zone': ['undefined', 'non-move', 'over-maxhr'], 'label': [
                                'undefined', 'non-move', 'over-maxhr'], 'lowvar': [0, 0, 0], 'upvar': [0, 0, 0], 'lower': [0, 40, hr_zone.at[4, 'upper']], 'upper': [40, hr_zone.at[0, 'lower'], 0]})
            hr_zone = hr_zone.append(temp, ignore_index=True)
        else:
            hr_zone = 0

        return hr_zone

    def calcStepsCluster(self, userid, steps_dataset):
        if len(steps_dataset) > 0:
            data = steps_dataset[steps_dataset.type ==
                                 4].copy().reset_index(drop=True)
            if len(data) > 0:
                clust_steps = pd.DataFrame(
                    {'uid': [userid], 'steps': data['value'].sum()})
            else:
                clust_steps = pd.DataFrame({'uid': [userid], 'steps': 0})
        else:
            clust_steps = pd.DataFrame({'uid': [userid], 'steps': 0})

        return clust_steps

    def calcStepsDuration(self, steps_dataset):
        if len(steps_dataset) > 0:
            data = steps_dataset[steps_dataset.type ==
                                 4].copy().reset_index(drop=True)
            data['dur_sec'] = 0
            if len(data) > 0:
                data['dur_sec'] = (data['to'] - data['from']
                                   ).apply(lambda x: pd.Timedelta(x).seconds)
        return data

    def calcHeartRateCluster(self, userid, hr_dataset, hr_zone):
        if len(hr_dataset) > 0:
            data = hr_dataset[hr_dataset.type ==
                              7].copy().reset_index(drop=True)
            row_recs = len(data)
            if (row_recs > 0):
                clust_hr = pd.DataFrame({'uid': [userid], 'hr_max': data.value.max(
                ), 'hr_over': ((data.value > 100).sum() * 100 / row_recs).round(decimals=0), 'hr_label': 'normal'})
                for i in hr_zone.index:
                    if (clust_hr.at[0, 'hr_max'] >= hr_zone.at[i, 'lower']) & (clust_hr.at[0, 'hr_max'] < hr_zone.at[i, 'upper']):
                        clust_hr.at[0, 'zone'] = hr_zone.at[i, 'zone']
                        break
                    elif (clust_hr.at[0, 'hr_max'] >= hr_zone.iloc[-1, 5]):
                        clust_hr.at[0, 'zone'] = hr_zone.iloc[-1, 2]
                        break

                clust_hr[['hr_max', 'hr_over']] = clust_hr[['hr_max', 'hr_over']].apply(
                    pd.to_numeric, errors='coerce')
            else:
                clust_hr = pd.DataFrame(
                    {'uid': [userid],  'hr_max': 0, 'hr_over': 0, 'hr_label': 'non-wear', 'zone': 'no-data'})
        else:
            clust_hr = pd.DataFrame(
                {'uid': [userid],  'hr_max': 0, 'hr_over': 0, 'hr_label': 'non-wear', 'zone': 'no-data'})

        return clust_hr

    def getDailyReport(self, userId: str, ts: datetime):
        daily_col = self.report_db[constant.DAILY_COLL]
        query = {
            'userId': userId,
            'category': EDailyReportCategory.Indicator.value,
            'ts': ts
        }
        print(query)
        daily = daily_col.find_one(query)
        return daily

    def createDailyReport(self, userId: str, ts: datetime):
        daily_col = self.report_db[constant.DAILY_COLL]
        #  generate clusters has 10 minutes period
        clusters = utils.generateCluster(ts, 10 * 60 * 1000)
        # create daily report
        dailyData = {
            'userId': userId,
            'category': EDailyReportCategory.Indicator.value,
            'ts': ts,
            'clusters': clusters
        }
        # insert daily report
        return daily_col.insert_one(dailyData)

    def updateDailyReport(self, id: ObjectId, clusters: list):
        daily_col = self.report_db[constant.DAILY_COLL]
        query = utils.correct_encoding({'_id': id})
        data = utils.correct_encoding({'$set': {'clusters': clusters}})
        result = daily_col.update_one(query, data)
        return result

    def calcHeartRateZone(self, userid, hr_dataset, hr_zone):
        hr_zone_dur = pd.DataFrame()
        if len(hr_dataset) > 0:
            data = hr_dataset[hr_dataset.type ==
                              7].copy().reset_index(drop=True)
            data['zone'] = np.nan
            data['dur_sec'] = 0
            if (len(data) > 0):
                for i in hr_zone.index:
                    case_zone = (data['value'] >= hr_zone.at[i, 'lower']) & (
                        data['value'] < hr_zone.at[i, 'upper'])
                    data.loc[case_zone, 'zone'] = hr_zone.at[i, 'zone']
                if (data['to'] - data['from']).apply(lambda x: pd.Timedelta(x).seconds).all() == 0:
                    data.loc[data['dur_sec'] == 0, 'dur_sec'] = 1
                else:
                    data['dur_sec'] = (data['to'] - data['from']
                                       ).apply(lambda x: pd.Timedelta(x).seconds)

                hr_zone_dur = data.groupby(['userId', 'type', 'date', 'zone'], as_index=False)[
                    'dur_sec'].sum()

            else:
                hr_zone_dur = []
        else:
            hr_zone_dur = []
        return hr_zone_dur

    def calcHeartRateMetrics(self, clust_hr, clust_steps):
        data = pd.merge(
            clust_hr, clust_steps[['uid', 'steps']],  how='left', on=['uid'])
        data.loc[data['hr_over'] >= 50, 'hr_label'] = 'abnormal'
        conditions = [
            (data['hr_over'] >= 50) & (data['steps'] == 0),
            (data['hr_over'] >= 50) & (data['steps'] > 0),
            (data['hr_max'] > 0) & (data['steps'] == 0),
            (data['hr_max'] == 0) & (data['steps'] > 0)]
        events = ['abnormal', 'walking-hr', 'resting-hr', 'non-wear']
        data['hr_label'] = np.select(conditions, events, default='normal')

        return data

    def isHeartRateOver(self, hr_over: int):
        return hr_over >= 100

    def createNotification(self,  start_date: datetime, end_date: datetime, userId: str, data: dict):
        title = 'High heart rate notification'

        tz_utc = pytz.timezone('UTC')
        tz_bangkok = pytz.timezone('Asia/Bangkok')
        bangkok_time = tz_utc.localize(start_date)
        bangkok_time = bangkok_time.astimezone(tz_bangkok)
        startTime = bangkok_time.strftime("%H:%M")

        body = f"อัตราการเต้นหัวใจของคุณสูงกว่า 100 BPM ขณะที่คุณไม่ได้เคลื่อนไหวเป็นเวลา 10 นาที่ ตั้งแต่เวลา {startTime}."

        print('start date', start_date.isoformat(), startTime)
        receiverIds = [userId]
        hs.createNotificationTask(
            'HighHeartRate', end_date, receiverIds, title, body, 6, data)
