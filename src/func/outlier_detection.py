import numpy as np
import pandas as pd
import constant
from constant import EHealthType, HEALTH_ANALYTIC_COLL, HEALTH_RECORD_DETECT_OUTLIER_PROCESS_NAME, EAnalyticCategory
import func.utils as utils
import json
import logging
import traceback


class HealthOutlier:
    def __init__(self, report_db):
        # self.connection = DatabaseConnect()
        # self.report_db = self.connection.getReportDB()
        self.report_db = report_db

    def process(self, data):
        try:
            health_raw_data = pd.DataFrame(data)
            start = data[0]['to']
            end = data[len(data) - 1]['to']

            type_list = health_raw_data['type'].unique()
            type_list.sort
            boxParamList = []
            sliceData = []
            outlierData = []
            temp = []
            nameT = []
            valid_type = ['value3', 'value2', 'value']
            for val in valid_type:
                for enum in type_list:
                    sliceData = health_raw_data[val][health_raw_data.type == enum]
                    box_param, label = self.outlier_treatment(
                        sliceData, enum, val)
                    temp = health_raw_data[health_raw_data.type == enum].copy(
                        deep=True)
                    temp['label' + val] = label
                    # print("<<xxxx>>")
                    outlierData = temp.append(
                        outlierData, ignore_index=True)
                    boxParamList = box_param.append(
                        boxParamList, ignore_index=True)

            boxParamList = boxParamList.sort_values(
                ['valType', 'type'], ascending=[True, True])
            boxParamList = boxParamList.set_index('valType')
            boxParamJson = {}
            for idx in valid_type:
                temp = json.loads(
                    boxParamList[boxParamList.index == idx].to_json(orient='records'))
                boxParamJson.update({idx: temp})

            outlierData = outlierData.set_index('_id')
            outlierListFilt = []
            outlierListFilt = outlierData[(outlierData.labelvalue.isin(['ex_outlier', 'outlier'])) | (outlierData.labelvalue2.isin(
                ['ex_outlier', 'outlier'])) | (outlierData.labelvalue3.isin(['ex_outlier', 'outlier']))][['userId', 'type', 'labelvalue', 'labelvalue2', 'labelvalue3']]
            result = outlierListFilt.to_json(orient='records')
            outlierListTJson = json.loads(result)

            info = self.filterInfo(health_raw_data)
            info = utils.dfToJson(info)

            health_dict = {
                'category': EAnalyticCategory.HEALTH_OUTLIER.value,
                'start': start,
                'end': end,
                'info': info,
                'boxQuat': boxParamJson,
                'outlier': outlierListTJson}

            self.create_health_info(health_dict)

            return False
        except Exception as e:
            logging.error('Thread %s: Error => {}'.format(
                e), HEALTH_RECORD_DETECT_OUTLIER_PROCESS_NAME)
            traceback.print_exc()
            tb = traceback.format_exc()
            return tb

    # box_param, label  = outlier_treatment(datacolumn,'all') <= call function

    def outlier_treatment(self, datacolumn, healthType, valType):
        Q1, Q3 = np.percentile(datacolumn, [25, 75])
        IQR = Q3 - Q1
        in_lower_range = Q1 - (1.5 * IQR)
        in_upper_range = Q3 + (1.5 * IQR)
        out_lower_range = Q1 - (3.0 * IQR)
        out_upper_range = Q3 + (3.0 * IQR)

        outlier_inidx = list(
            datacolumn[(datacolumn > in_upper_range) | (datacolumn < in_lower_range)].index)
        outlier_outidx = list(
            datacolumn[(datacolumn > out_upper_range) | (datacolumn < out_lower_range)].index)

        datacolumn_df = pd.DataFrame(datacolumn)
        datacolumn_df['label'] = 'normal'

        datacolumn_df.loc[outlier_inidx, 'label'] = 'outlier'
        datacolumn_df.loc[outlier_outidx, 'label'] = 'ex_outlier'

        bp_data = []
        box_param = {}
        box_param['valType'] = valType
        box_param['type'] = healthType
        box_param['outLowerRange'] = out_lower_range
        box_param['inLowerRange'] = in_lower_range
        box_param['lowerQuartile'] = Q1
        box_param['median'] = (Q3 + Q1)/2
        box_param['upperQuartile'] = Q3
        box_param['inUpperRange'] = in_upper_range
        box_param['outUpperRange'] = out_upper_range
        bp_data.append(box_param)

        label = pd.Series(datacolumn_df.label)

        return pd.DataFrame(bp_data), label

    # outlier_idx, label_zscore = z_score_outier_detection(datacolumn) <= call function

    # def z_score_outier_detection(self, var):
    #     threshold = 3  # 3 standard deviation
    #     var_mean = np.mean(var)
    #     var_sd = np.std(var)
    #     z_scores = [(i - var_mean) / var_sd for i in var]
    #     outlier_idx = np.where(np.abs(z_scores) > threshold)
    #     label_zscore = []
    #     label_zscore = pd.DataFrame(var)
    #     label_zscore['label'] = 'normal'
    #     label_zscore['label'].iloc[outlier_idx] = 'outlier'
    #     return outlier_idx, pd.DataFrame(label_zscore.label)

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

    def create_health_info(self, data):
        health_analytic_col = self.report_db[HEALTH_ANALYTIC_COLL]
        data = utils.correct_encoding(data)
        result = health_analytic_col.insert_one(data)
