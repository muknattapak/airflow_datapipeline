from func.database_connect import DatabaseConnect
from func.email_service import EmailService
import func.utils as utils
import constant
import json
import datetime
import logging
import math

# import constant


class AnalyticTask:
    def __init__(self, health_db, sl_db):
        # self.connection = DatabaseConnect()
        self.health_db = health_db
        self.sl_db = sl_db  # = self.connection.getReportDB()

    def get_config(self, name):
        config_col = self.sl_db[constant.ANALYTIC_CONFIG_COLL]

        config = config_col.find_one({"name": name}, {'errMsg': 0})

        if config is None:
            return None

        if config["active"] == False:
            config_col.update_one({"_id": config["_id"]},
                                  {"$set": {
                                      "isProcess": False
                                  }})
            return None

        if config['errCount'] >= config['errLimit']:
            return None

        config_col.update_one({"_id": config["_id"]},
                              {"$set": {
                                  "isProcess": True
                              }})
        return config

    def update_config(self, config, data):
        item = data[len(data) - 1]
        recordDone = config["recordDone"] + len(data)

        dataUpdate = {
            "latestTimestamp": item[config["timestampField"]],
            "recordDone": recordDone,
            "latestId": item["_id"],
            "isProcess": True,
        }

        config_col = self.sl_db[constant.ANALYTIC_CONFIG_COLL]
        config_col.update_one({"_id": config["_id"]}, {"$set": dataUpdate})
        return dataUpdate

    def update_config_custom(self, config, dataUpdate):
        # item = data[len(data) - 1]
        # recordDone = config["recordDone"] + len(data)

        # dataUpdate = {
        #     "latestTimestamp": item[config["timestampField"]],
        #     "recordDone": recordDone,
        #     "latestId": item["_id"],
        #     "isProcess": True,
        # }

        config_col = self.sl_db[constant.ANALYTIC_CONFIG_COLL]
        config_col.update_one({"_id": config["_id"]}, {"$set": dataUpdate})
        return dataUpdate

    def update_error(self, config, err):
        errCount = config['errCount'] + 1
        dataUpdate = {
            "isProcess": False,
            "errMsg": str(datetime.datetime.now()) + ': err => ' + str(err),
            "errCount": errCount
        }
        config_col = self.sl_db[constant.ANALYTIC_CONFIG_COLL]
        data = utils.correct_encoding({"$set": dataUpdate})
        config_col.update_one({"_id": config["_id"]}, data)

        # เช็คถ้า state error ให้ทำการ count error หากครบ 10 รอบ แล้วให้หยุด service
        # จากนั้นทำการส่ง email หาผู้ดูแล
        # แค่ถ้า สำเร็จ ให้ reset count

        # send email
        if errCount == config['errLimit']:
            email_service = EmailService()
            email_service.send_error(
                config['name'], dataUpdate)

    # data feed
    def get_health_data(self, name, config):
        # connect to DB
        # health_db = self.connection.getHealthDB()
        health_record_col = self.health_db[config["collection"]]
        query = {config["timestampField"]: {"$gt": config["latestTimestamp"]}}
        print('query', query)
        print('sort', config["timestampField"], int(config["sort"]))
        print('limit', int(config["limit"]))

        cursor = health_record_col.find(query).sort(config["timestampField"],
                                                    int(config["sort"])).limit(
                                                        int(config["limit"]))
        data = list(cursor)
        return data

    def get_health_record_group_by_userId(self, config, start, end, types, limit, index):
        health_record_col = self.health_db[config["collection"]]

        pipeline = [
            {
                '$match': {
                    # 'userId': '5eaa7b4d079a460011315d3f',
                    'to': {
                        '$gt': start,
                        '$lte': end
                    },
                    'type': {'$in': types}
                }
            },
            {
                '$group': {
                    '_id': '$userId',
                    'healths': {'$push': '$$ROOT'}
                }
            },
            {
                '$facet': {
                    'paginatedResults': [{'$skip': index}, {'$limit': limit}],
                    'totalCount': [
                        {
                            '$count': 'count'
                        }
                    ]
                }
            }
        ]
        # query = {config["timestampField"]: {"$gte": config["latestTimestamp"]}}
        # query = { **query, **option }
        # print('query', query)
        # print('sort', config["timestampField"], int(config["sort"]))
        # print('limit', int(config["limit"]))

        cursor = health_record_col.aggregate(pipeline)
        data = list(cursor)

        results = data[0]['paginatedResults']
        totalCount = 0
        if len(data[0]['totalCount']) > 0:
            totalCount = data[0]['totalCount'][0]['count']

        totalPage = math.ceil(totalCount / limit)

        nextPage = {
            'limit': limit,
            'index': index + 1,
        }

        print('nextPage', math)

        if (index + 1 >= totalPage):
            nextPage = None

        response = {
            'data': results,
            'total': totalCount,
            'nextPage': nextPage
        }

        return response
