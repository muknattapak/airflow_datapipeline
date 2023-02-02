from sqlalchemy import create_engine, DDL, Column, Integer, String, DateTime, Sequence, Text
from sqlalchemy.ext.declarative import declarative_base
import sqlalchemy as sa
import pandas as pd


def connection():
    engine = create_engine(
        'postgresql://airflow:airflow@es.aidery.io:5433/postgres')
    return engine


engine = connection()

Base = declarative_base()

table_name = 'domains'


with engine.begin() as conn:
    # step 0.0 - create test environment
    conn.exec_driver_sql("DROP TABLE IF EXISTS main_table")
    conn.exec_driver_sql("DELETE FROM temp_table")
    conn.exec_driver_sql(
        "CREATE TABLE main_table (id int primary key, txt varchar(50))"
    )
    conn.exec_driver_sql(
        "INSERT INTO main_table (id, txt) VALUES (1, 'row 1 old text')"
    )
    # step 0.1 - create DataFrame to UPSERT
    df = pd.DataFrame(
        [(1, "new row 4 text"), (2, "row 3 new text")], columns=["id", "txt"]
    )

    # step 1 - create temporary table and upload DataFrame
    conn.exec_driver_sql(
        "CREATE TEMPORARY TABLE temp_table AS SELECT * FROM main_table WHERE false"
    )
    df.to_sql("temp_table", conn, index=False, if_exists="append")

    # step 2 - merge temp_table into main_table
    conn.exec_driver_sql(
        """\
        INSERT INTO main_table (id, txt) 
        SELECT id, txt FROM temp_table
        ON CONFLICT (id) DO
            UPDATE SET txt = EXCLUDED.txt
        """
    )

    # step 3 - confirm results
    result = conn.exec_driver_sql("SELECT * FROM main_table ORDER BY id").all()
    print(result)  # [(1, 'row 1 new text'), (2, 'new row 2 text')]
