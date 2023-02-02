from bson.json_util import dumps
from func.database_connect import DatabaseConnect
import func.utils as utils
import pandas as pd
import numpy as np
import constant
from constant import EHealthType, EAnalyticCategory
import json
import logging
import traceback


class HealthValidate:
    def __init__(self, report_db):
        # self.connection = DatabaseConnect()
        # self.report_db = self.connection.getReportDB()
        self.report_db = report_db

    def process(self, data):
        try:
            health_data = pd.DataFrame(data)
            type_list = health_data['type'].unique()

            # for enum in type_list:
            #     health_data.loc[health_data['type'] ==
            #                     enum, 'type'] = EHealthType(enum).name

            info = self.filterInfo(health_data)
            overview = self.findDescriptionDataset(health_data)

            info = utils.dfToJson(info)
            overview = utils.dfToJson(overview)

            start = data[0]['to']
            end = data[len(data) - 1]['to']

            health_dict = {
                'category': EAnalyticCategory.OVERVIEW.value,
                'start': start,
                'end': end,
                'info': info,
                'overview': overview}
            self.create_health_info(health_dict)

            return False
        except Exception as e:
            logging.error('Thread %s: Error => {}'.format(
                e), constant.HEALTH_RECORD_PRE_PROCESS_NAME)
            traceback.print_exc()
            tb = traceback.format_exc()
            return tb

    def filterInfo(self, health_data):
        # ========= filter information ===============

        health_dataInfo = []
        countTotal = pd.DataFrame(health_data.count(
            axis='rows'), columns=['count_total'])
        dType = pd.DataFrame(health_data.dtypes.map(
            lambda x: x.name), columns=['data_type'])
        # dType = pd.DataFrame(health_data.dtypes, columns=['data_type'])

        # count duplicate value per column
        countNull = pd.DataFrame(
            health_data.isnull().sum(), columns=['count_null'])
        # countDup = pd.DataFrame(health_data.apply(lambda x: x.duplicated()).sum(
        # ), columns=['count_dup'])

        countDup = pd.DataFrame(health_data.apply(lambda x: utils.duplicate_value(
            x)), columns=['count_dup'])  # count duplicate value per column

        isNeg = pd.DataFrame(health_data.select_dtypes(np.number).lt(0).any()
                             .reindex(health_data.columns, fill_value=False), columns=['isneg'])
        health_dataInfo = pd.concat(
            [countTotal, dType, countNull, countDup, isNeg], axis=1)
        return health_dataInfo

    def findDescriptionDataset(self, health_data):
        # ======= descriptive dataset =================
        # health_dataOverview = []
        # health_data.loc[health_data.type.isin([options[0]])][['type','value', 'value2', 'value3']]
        data_temp = health_data[['type', 'value', 'value2', 'value3']]
        data_desc_temp = data_temp.groupby(['type']).describe()

        return data_desc_temp

    def create_health_info(self, data):
        health_analytic_col = self.report_db[constant.HEALTH_ANALYTIC_COLL]
        data = utils.correct_encoding(data)
        result = health_analytic_col.insert_one(data)
