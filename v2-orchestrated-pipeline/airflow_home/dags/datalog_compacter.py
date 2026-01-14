# TODO:
# [] Read files from S3 and list keys of multiple files in a single partition
# [] Reprocess those files and repartition
# [] Delete the old files
import logging
from datetime import datetime

# ====== helper functions ======
def list_smallfile_partitions():
    logger = logging.getLogger(__name__)




# ====== main dag ======
@dag(
    dag_id="datalog_compacter",
    description="Compact small files into one file per partition",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False,
    params={
    },
    tags=[]
)
def compact():
