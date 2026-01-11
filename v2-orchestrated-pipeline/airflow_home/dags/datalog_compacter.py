from contextlib import contextmanager
from pathlib import Path
import logging
from datetime import datetime, timedelta

import duckdb
from sqlalchemy import text
from airflow.sdk import dag, task
from airflow.models import Param
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook

# ====== GLOBAL VARIABLES ======
BUCKET_NAME = "smartdbucket"


# ====== FUNCTION DECLARATIONS =======
@contextmanager
def init_duckdb_connection(aws_credentials: dict, ram_limit: str):
    logger = logging.getLogger(__name__)

    existing_keys = list(aws_credentials.keys())
    required_keys = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
    if not set(required_keys) <= set(existing_keys):
        logger.exception(
            f"AWS Credentials doesn't contain required keys {required_keys}"
        )
        raise

    try:
        logger.info("Initializing duckdb connection to S3")
        conn = duckdb.connect()
        conn.execute("INSTALL httpfs;")
        conn.execute("LOAD httpfs;")
        conn.execute(f"SET memory_limit = '{ram_limit}'")
        conn.execute(f"SET s3_region = '{aws_credentials['aws_region']}';")
        conn.execute(
            f"SET s3_access_key_id = '{aws_credentials['aws_access_key_id']}';"
        )
        conn.execute(
            f"SET s3_secret_access_key = '{aws_credentials['aws_secret_access_key']}';"
        )

        yield conn
    finally:
        conn.close()


def get_pending_keys_sql(engine, distrik, file_limit=1000):
    logger = logging.getLogger(__name__)
    logger.info(f"Getting log files S3 keys to compress with limit: {file_limit}")

    if distrik == "BRCB":
        query = text(
            f"""SELECT TOP {file_limit} file_path_s3 
                     FROM tbl_t_upload_datalog 
                     WHERE is_upload_s3 = 'true'
                        AND distrik = 'BRCB'
                        AND file_path_lokal != 'Minio'
                        AND (compression_status != 'SUCCESS'  OR compression_status IS NULL)
                        AND upload_s3_date >= '2025-12-01 00:00'
                     ORDER BY upload_s3_date DESC
                     """
        )
    elif distrik == "BRCG":
        query = text(
            f"""SELECT TOP {file_limit} file_name
                        FROM tbl_t_upload_s3_log
                        WHERE distrik = 'BRCG'
                            AND (compression_status IS NULL OR compression_status != 'SUCCESS')
                            AND status = 'OK'
                            AND upload_date >= '2025-12-01 00:00'
                        ORDER BY upload_date DESC
                    """
        )
    else:
        logger.exception("District variable not in 'BRCB' OR 'BRCG'")
        raise Exception

    with engine.connect() as conn:
        result = conn.execute(query)
        list_of_keys = [row[0] for row in result]

    row_count = len(list_of_keys)

    if row_count == 0:
        logger.info("No more pending data to process!")
        return []

    logger.info(f"Got {row_count} of keys to work on.")
    return list_of_keys


def get_datalog_from_s3_per_hiveperiod(
    conn, bucket_name: str, s3key_list: list, targetpath: str, distrik: str
):
    logger = logging.getLogger(__name__)

    logger.info("Grabbing datalog for device all from s3")

    s3key_list_string = (
        f"['s3://{bucket_name}/" + f"', 's3://{bucket_name}/".join(s3key_list) + "']"
    )
    print(s3key_list_string[:100])

    query = f"""
        SELECT 
            *,
            '{distrik}' AS dstrct_code,
            CAST(
                CASE
                    WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                    WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                    WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                    ELSE make_timestamp(CAST(heartbeat / 1000as BIGINT))
                END + INTERVAL 8 HOURS
            AS DATE) as hiveperiod,
            CAST(
                CASE
                    WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                    WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                    WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                    ELSE make_timestamp(CAST(heartbeat / 1000as BIGINT))
                END + INTERVAL 8 HOURS
            AS DATETIME) as datetime_wita,
            filename AS source_file
        FROM read_json_auto({s3key_list_string}, filename=true, sample_size=-1, union_by_name=true)
    """

    data = conn.sql(query)

    logger.info("Got the main data from s3")

    row_count = data.count("*").fetchone()[0]

    if row_count == 0:
        logger.warning(f"No data found for {s3key_list_string}")

        return None

    logger.info(f"Writing parquet file to target with {row_count} rows")

    filename_pattern = "standard_{uuid}"
    main_query = f"""
        COPY (SELECT * FROM data)
        TO '{targetpath}/datalog_v2' 
        (
            FORMAT parquet,
            COMPRESSION snappy,
            PARTITION_BY (hiveperiod,dstrct_code),
            FILENAME_PATTERN '{filename_pattern}',
            APPEND
        )
    """
    try:
        conn.execute(main_query)
    except Exception:
        logger.exception("Main compacter query failed!")
        raise

    logger.info("Writing metadata to conversion log")

    logger.info("All done!")

    return None


def update_compression_status_in_db(engine, keys: list, distrik: str):
    logger = logging.getLogger(__name__)
    row_num = len(keys)
    logger.info(f"Updating success status for {row_num} keys")
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    key_list_string = "','".join(keys)

    if distrik == "BRCB":
        query = text(
            f"""UPDATE tbl_t_upload_datalog
                        SET compression_status = 'SUCCESS', compression_timestamp = '{now}'
                        WHERE file_path_s3 IN ('{key_list_string}')
                     """
        )

    elif distrik == "BRCG":
        query = text(
            f"""UPDATE tbl_t_upload_s3_log
                        SET compression_status = 'SUCCESS', compression_timestamp = '{now}'
                        WHERE file_name IN ('{key_list_string}') and status = 'OK'
                     """
        )

    else:
        logger.exception("District variable not in 'BRCB' OR 'BRCG'")
        raise Exception

    with engine.connect() as conn:
        result = conn.execute(query)
        conn.commit()

    return result


# ====== DAG DEFINITION ======
@dag(
    dag_id="s3_data_compacter",
    schedule=timedelta(hours=1),
    start_date=datetime(2026, 1, 5),
    params={
        "distrik": Param("BRCB", enum=["BRCG", "BRCB"]),
        "key_limit_per_run": Param(1000, type="integer"),
        "ram_limit": Param("10GB", type="string"),
        "target_path": Param(
            "s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania",
            enum=["data", "s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania"],
        ),
    },
)
def compacter():

    @task
    def get_keys():
        hook = MsSqlHook(mssql_conn_id="mssql_pama_compacter")
        hook.get_sqlalchemy_engine()
        keys = get_pending_keys_sql(
            engine, params["distrik"], params["key_limit_per_run"]
        )

        return keys

    @task
    def compact(keys: list):
        params = context["params"]
        if len(keys) == 0:
            return 0

        with init_duckdb_connection(aws_creds, params["ram_limit"]) as conn:
            get_datalog_from_s3_per_hiveperiod(
                conn, BUCKET_NAME, keys, params["target_path"], params["distrik"]
            )

    @task
    def update_status(keys: list):
        """Mark keys as processed in SQL Server."""
        if len(keys) == 0:
            return 0

        params = context["params"]

        mssql_hook = MsSqlHook(mssql_conn_id="mssql_pama")
        engine = mssql_hook.get_sqlalchemy_engine()

        update_compression_status_in_db(engine, keys, params["distrik"])

    keys = get_keys()
    compact(keys)
    update_status(keys)


compacter()
