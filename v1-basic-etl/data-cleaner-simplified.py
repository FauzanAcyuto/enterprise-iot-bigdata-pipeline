import json
import logging
from contextlib import contextmanager
from logging.handlers import RotatingFileHandler
from pathlib import Path

import boto3
import duckdb
import polars as pl

# ======= CONFIGURATION =======
CREDENTIALS_PATH = "../creds/creds.json"
with open(Path(CREDENTIALS_PATH), "r") as file:
    creds = json.load(file)
    cred = creds["AWS"]

keys_to_keep = ["aws_secret_access_key", "aws_access_key_id", "aws_region"]
aws_creds = {key: value for key, value in cred.items() if key in keys_to_keep}

BUCKET_NAME = "smartdbucket"
PREFIX = "datalog/cis_smartd_tbl_iot_scania/"
TARGET_BUCKET_PATH = "s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania"

# logging parameters
LOG_LEVEL = logging.INFO
LOG_FILE_PATH = "logs/data-cleaner.log"
LOG_SIZE_MB = 1
LOG_FILES_TO_KEEP = 5


def setup_logger(loglevel, log_file, logsize, files_to_keep):
    """
    loglevel(obj) = log level object (logging.INFO)
    log_file(str) = path to log file (../log/etl.log)
    logsize(int) = size of log files before rotated (in MB)
    files_to_keep = number of rotated files to keep
    """
    # Create log directory
    Path(log_file).parent.mkdir(parents=True, exist_ok=True)

    # Setup logging
    logging.basicConfig(
        handlers=[
            RotatingFileHandler(
                log_file,
                maxBytes=logsize * 1024 * 1024,
                backupCount=files_to_keep,
            ),
            logging.StreamHandler(),
        ],
        level=loglevel,
        format="%(asctime)s - %(levelname)s - %(message)s",
        force=True,  # Override any existing config
    )

    logger = logging.getLogger()
    logger.info(f"Logging initialized → {log_file}")
    return logger


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
        conn.execute("SET TimeZone = 'UTC';")
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


def get_invalid_hiveperiods(target_file_path: str):
    with init_duckdb_connection(aws_creds, "4GB") as conn:
        data = conn.sql(
            """
            SELECT dstrct_code, hiveperiod,heartbeat, CAST(
            CASE
                WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                ELSE make_timestamp(CAST(heartbeat / 1000as BIGINT))
            END + INTERVAL 8 HOURS
        AS DATE) as accurate_wita_date
            FROM read_parquet('s3://smartdbucket/datalog/cis_smartd_tbl_iot_scania/**/*.parquet',hive_partitioning=true)
            """
        )

        df = conn.sql(
            """
            SELECT DISTINCT hiveperiod,dstrct_code,accurate_wita_date
            FROM data
            WHERE hiveperiod != accurate_wita_date
        """
        ).pl()

        df.write_csv(target_file_path)

        return df.get_column("hiveperiod").to_list()


def get_keys(creds, bucketname, prefix, daterange):
    logger = logging.getLogger(__name__)

    logger.info("Initiating key acquisition")

    try:
        client = boto3.client(
            "s3",
            aws_access_key_id=creds["aws_access_key_id"],
            aws_secret_access_key=creds["aws_secret_access_key"],
            region_name=creds["aws_region"],
        )
    except Exception:
        logger.exception("Error when authenticating to S3")

    paginator = client.get_paginator("list_objects_v2")
    pages = paginator.paginate(Bucket=bucketname, Prefix=prefix)
    data = []
    for page in pages:
        for obj in page["Contents"]:
            data.append(obj)
    logger.info(f"Obtained {len(data)} rows of keys from S3")
    df = pl.from_dicts(data)

    df = df.with_columns(
        pl.col("Key")
        .str.splitn("/", 7)
        .alias("list_substr_key")
        .struct.rename_fields(
            ["prefix1", "prefix2", "prefix3", "hiveperiod", "dstrct_code", "filename"]
        )
        .alias("fields")
        # pl.col("Key").str.split("/").list.get(2).alias("hiveperiod"),
        # pl.col("Key").str.split("/").list.get(3).alias("dstrct_code"),
        # pl.col("Key").str.split("/").list.get(4).alias("filename"),
    ).unnest("fields")

    df = df.filter(
        pl.col("hiveperiod")
        .str.replace("hiveperiod=", "")
        .is_between(daterange[0], daterange[1])
    ).select("Key")
    logger.info(f"final dataframe length: {len(df)}")
    return df.get_column("Key").to_list()


def reprocess_s3_data(conn, bucket_name: str, s3key_list: list, targetpath: str):
    logger = logging.getLogger(__name__)

    logger.info("Grabbing datalog for device all from s3")

    s3key_list_string = (
        f"['s3://{bucket_name}/" + f"', 's3://{bucket_name}/".join(s3key_list) + "']"
    )
    print(s3key_list_string[:100])
    data = conn.sql(
        f"""
        SELECT 
            * EXCLUDE (hiveperiod),
            CAST(
            CASE
                WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                ELSE make_timestamp(CAST(heartbeat / 1000 as BIGINT))
            END + INTERVAL 8 HOURS
        AS DATE) as hiveperiod,
            CAST(
            CASE
                WHEN heartbeat < 10000000000 THEN make_timestamp(CAST(heartbeat * 1000000 as BIGINT) )
                WHEN heartbeat < 10000000000000 THEN make_timestamp(CAST(heartbeat * 1000 as BIGINT))
                WHEN heartbeat < 10000000000000000 THEN make_timestamp(CAST(heartbeat as BIGINT))
                ELSE make_timestamp(CAST(heartbeat / 1000 as BIGINT))
            END + INTERVAL 8 HOURS
        AS DATETIME) as datetime_wita
        FROM read_parquet({s3key_list_string},hive_partitioning=true)
    """
    )

    logger.info("Got the main data from s3")

    row_count = data.count("*").fetchone()[0]

    if row_count == 0:
        logger.warning(f"No data found for {s3key_list_string}")

        return None

    logger.info(f"Writing parquet file to target with {row_count} rows")
    filename_pattern = "cleaned_{uuid}"
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


def main():
    keys_list = get_keys(aws_creds, BUCKET_NAME, PREFIX)
    with init_duckdb_connection(aws_creds, "4GB") as conn:
        reprocess_s3_data(conn, BUCKET_NAME, keys_list[10:15], Path("../data/"))


if __name__ == "__main__":
    main()
