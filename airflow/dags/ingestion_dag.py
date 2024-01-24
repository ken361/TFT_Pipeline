from datetime import datetime, timedelta
from ingestion_functions import *

from airflow.decorators import dag, task
from airflow.providers.google.operators.dataproc import DataprocCreateClusterOperator
from airflow.providers.google.operators.dataproc import DataprocDeleteClusterOperator
from airflow.providers.google.operators.dataproc import DataprocSubmitPySparkJobOperator

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

CLUSTER_NAME = 'tftpipeline-spark-cluster'
CLUSTER_REGION = 'northamerica-northeast2'
PYSPARK_FILE = 'spark_all_matches.py'

BQ_DATASET = 'tft_matches_all'
BQ_TABLE = 'tft_matches'
TFT_UNIT_TABLE = 'tft_unit_rarity'
JOB_ARGS = [PROJECT_ID, BQ_DATASET, BQ_TABLE, TFT_UNIT_TABLE, BUCKET]

CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50},
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50},
    },
}

default_args = {
    'owner': 'airflow',
    'depends_on_past': True,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

@dag(
    default_args=default_args, 
    schedule_interval='0 0 * * *', 
    start_date=datetime(2024, 1, 10), 
    catchup=False)
def ingestion_dag():
    @task()
    def check_patch():
        return get_current_patch()
    
    @task()
    def upload_matches_to_gcs(curr_patch: str):
        """ Retrieve match details and upload the parquetized files to GCS. """
        api = configure_api()
        summoner_ids = get_summonerIds(api)
        puuids = get_puuids(api, summoner_ids)
        all_matches = get_unique_match_ids(api, puuids)
        fetch_and_upload_match_details(api, all_matches, curr_patch, BUCKET)

    create_dataproc_cluster_task = DataprocCreateClusterOperator(
        task_id='create_dataproc_cluster_task',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        cluster_config=CLUSTER_CONFIG,
        region=CLUSTER_REGION,
        trigger_rule='all_success'
    )

    submit_spark_job_task = DataprocSubmitPySparkJobOperator(
        task_id = "submit_dataproc_spark_job_task",
        main = f"gs://{BUCKET}/{PYSPARK_FILE}",
        arguments = JOB_ARGS,
        cluster_name = CLUSTER_NAME,
        region = CLUSTER_REGION,
        dataproc_jars = ["gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.0-preview.jar"]
    )

    delete_dataproc_cluster_task = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster_task',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=CLUSTER_REGION,
        trigger_rule='all_done'
    )

    check_patch_task = check_patch()
    upload_matches_task = upload_matches_to_gcs(curr_patch=check_patch_task)
    check_patch_task >> upload_matches_task >> create_dataproc_cluster_task >> submit_spark_job_task >> delete_dataproc_cluster_task

dag = ingestion_dag()
