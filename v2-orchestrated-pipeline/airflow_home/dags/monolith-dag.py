import json
import logging
from contextlib import contextmanager
from datetime import datetime,timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path
from time import sleep

import boto3
import duckdb
from sqlalchemy import URL, create_engine, text
from airflow.sdk import dag,task

@dag(
    dag_id="s3_data_compacter",
    schedule=timedelta(hours=1),
    start_date=datetime(2026,1,5),
    

)


