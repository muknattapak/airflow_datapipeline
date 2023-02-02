from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook


def _get_mongo_conn():

    return


def _get_postgres_conn():

    return


def _get_redis_conn():

    return


def _download_file(**context):
    ds = context["ds"]

    # Download file from S3
    s3_hook = S3Hook(aws_conn_id="minio")
    file_name = s3_hook.download_file(
        key=f"cryptocurrency/{ds}/shib.csv",
        bucket_name="datalake",
    )

    return file_name


def _load_data_into_database(**context):
    postgres_hook = PostgresHook(postgres_conn_id="postgres")
    conn = postgres_hook.get_conn()
    cursor = conn.cursor()

    # Get file name from XComs
    file_name = context["ti"].xcom_pull(
        task_ids="download_file", key="return_value")

    # Copy file to database
    postgres_hook.copy_expert(
        """
            COPY
                cryptocurrency_import
            FROM STDIN DELIMITER ',' CSV
        """,
        file_name,
    )
