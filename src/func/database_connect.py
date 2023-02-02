from pymongo import MongoClient
from pymongo.errors import ConnectionFailure
from settings.configuration import Configuration


class DatabaseConnect:
    def getSLCoreDB(self):
        # connect to database
        config = Configuration().getConfig()
        url = config.get("SL_DATABASE", "URL")
        dbName = config.get("SL_DATABASE", "DB")
        print('url', url)
        client = MongoClient(url, serverSelectionTimeoutMS=2000)
        try:
            db = client[dbName]
            print(dbName, 'database connected')
        except ConnectionFailure:
            print(dbName, "Server not available")
        return db

    def getAuthDB(self):
        # connect to database
        config = Configuration().getConfig()
        url = config.get("AUTH_DATABASE", "URL")
        dbName = config.get("AUTH_DATABASE", "DB")
        client = MongoClient(url, serverSelectionTimeoutMS=2000)
        try:
            db = client[dbName]
            print(dbName, 'database connected')
        except ConnectionFailure:
            print(dbName, "Server not available")
        return db

    def getHealthDB(self):
        # connect to database
        config = Configuration().getConfig()
        url = config.get("HEALTH_DATABASE", "URL")
        dbName = config.get("HEALTH_DATABASE", "DB")
        client = MongoClient(url, serverSelectionTimeoutMS=2000)
        try:
            db = client[dbName]
            print(dbName, 'database connected')
        except ConnectionFailure:
            print(dbName, "Server not available")
        return db

    def getReportDB(self):
        config = Configuration().getConfig()
        url = config.get("REPORT_DATABASE", "URL")
        dbName = config.get("REPORT_DATABASE", "DB")
        client = MongoClient(url, serverSelectionTimeoutMS=2000)
        try:
            db = client[dbName]
            print(dbName, 'database connected')
        except ConnectionFailure:
            print(dbName, "Server not available")
        return db

    def getReportLocalDB(self):
        db = "aidery-report"
        host = "localhost"
        port = "27017"

        client = MongoClient(host, int(port))
        try:
            db = client[db]
            print('database connected')
        except ConnectionFailure:
            print("Server not available")
        return db

    def getSLCoreDB(self):
        config = Configuration().getConfig()
        url = config.get("SL_DATABASE", "URL")
        dbName = config.get("SL_DATABASE", "DB")
        client = MongoClient(url, serverSelectionTimeoutMS=2000)
        try:
            db = client[dbName]
            print('database connected')
        except ConnectionFailure:
            print("Server not available")
        return db
