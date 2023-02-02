# from pymongo import MongoClient
# from datetime import datetime, timedelta
# from bson import ObjectId

import constant
from func.database_connect import DatabaseConnect
from func.database_post_connect import DatabasePostgConnect
from sqlalchemy import DDL, inspect

# ============ Create connection =============
connection = DatabaseConnect()

# ------------ connect to collection ------------
authDb = connection.getAuthDB()
# healthDb = connection.getHealthDB()
domainColl = authDb.get_collection(constant.AUTH_DOMAIN_COLL)
cursor = domainColl.find_one()

postConn = DatabasePostgConnect()
db = postConn.getPostgDB()


table_name = 'users'
query = DDL(" ".join(
    map(str, ['select * from ' + table_name + ' order by idx asc limit 10'])))
# query_col = DDL(" ".join(map(str, ['select * from users limit 1'])))
inspector = inspect(db)
inspector.get_columns('users')

cursor = list(db.execute(query))
df = pd.DataFrame(cursor)

col_name = list(db.execute(query_col).keys())  # get columns name from table


# cursor = domainColl.watch()
# document = next(cursor)


# pipeline = [
#     {"$match": {"fullDocument.username": "alice"}},
#     {"$addFields": {"newField": "this is an added field!"}},
# ]
# cursor = db.inventory.watch(pipeline=pipeline)
# document = next(cursor)


# result = healthColl.find(
#         {'userId':'60ebd27136ffa000118983ca', 'type':8})


# try:
#     # Only catch insert operations
#     with healthColl.watch([{'$match': {'operationType': 'insert'}}]) as stream:
#         for insert_change in stream:
#             print(insert_change)

# except pymongo.errors.PyMongoError:
#     # The ChangeStream encountered an unrecoverable error or the
#     # resume attempt failed to recreate the cursor.
#     logging.error('...')
