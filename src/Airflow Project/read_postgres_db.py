from func.database_post_connect import DatabasePostgConnect
from sqlalchemy import DDL
import pandas as pd
# from init_func import use_func_folder
# use_func_folder()


conn = DatabasePostgConnect()  # new postgres db connection class
db = conn.getPostgDB()  # get connection class properties

table_name = 'users'
query = DDL(" ".join(
    map(str, ['select * from ' + table_name + ' order by idx asc limit 10'])))
query_col = DDL(" ".join(map(str, ['select * from users limit 1'])))

cursor = list(db.execute(query))
df = pd.DataFrame(cursor)

col_name = list(db.execute(query_col).keys())  # get columns name from table
