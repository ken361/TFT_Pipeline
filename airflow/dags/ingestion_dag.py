import os
from datetime import datetime, timedelta

from ingestion_functions import (
    configure_api,
    get_summonerIds,
    get_puuids,
    fetch_and_upload_match_details,
    get_current_patch,
    check_patch_difference,
    delete_gcs_files,
    delete_bq_tables,
    get_unique_match_ids
)
from dd_ingestion_functions import (
    fetch_data_from_DD,
    format_data,
    upload_DD_data_to_gcs,
    upload_dd_data_to_bigquery
)

from airflow.decorators import dag, task
from airflow.providers.google.cloud.operators.dataproc import (
    DataprocCreateClusterOperator,
    DataprocDeleteClusterOperator,
    DataprocSubmitPySparkJobOperator,
)

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')
BUCKET_SUBDIR = 'pq'
BUCKET_SUBDIR_DD = 'id_to_name_pq'
TEMP_BUCKET = os.getenv('GCP_GCS_TEMP_BUCKET')
BQ_DATASET = os.getenv('GCP_BQ_DATASET')

CLUSTER_NAME = 'tftpipeline-spark-cluster'
CLUSTER_REGION = 'northamerica-northeast2'
PYSPARK_FILE = 'spark_all_matches.py'

UNITS_ALL_TABLE = 'units_played_all'
UNIT_RARITY_TABLE = 'unit_rarity'
TRAITS_TABLE = 'traits_played_all'
AUGMENTS_TABLE = 'augments_played_all'
JOB_ARGS = [
    PROJECT_ID,
    BQ_DATASET,
    UNITS_ALL_TABLE,
    UNIT_RARITY_TABLE,
    TRAITS_TABLE,
    AUGMENTS_TABLE,
    BUCKET,
    BUCKET_SUBDIR,
    TEMP_BUCKET
]

DD_ID_TO_NAME_TABLES = {
    'items': 'item_id_to_name',
    'champs': 'champ_id_to_name',
    'augments': 'augment_id_to_name',
    'traits': 'trait_id_to_name'
}

CLUSTER_CONFIG = {
    'master_config': {
        'num_instances': 1,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50
        },
    },
    'worker_config': {
        'num_instances': 2,
        'machine_type_uri': 'n1-standard-2',
        'disk_config': {
            'boot_disk_type': 'pd-standard', 'boot_disk_size_gb': 50
        },
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
    #-----------------------Data Dragon(DD) Ingestion-------------------------
    @task()
    def upload_id_to_name_mapping():
        """ Retrieve and upload data to map asset ids to names 
        from Riot Data Dragon (DD). 
        """
        base_url='https://ddragon.leagueoflegends.com/cdn/13.24.1/data/en_US/'
        dd_endpoints = {
            'items': 'tft-item.json',
            'champs': 'tft-champion.json',
            'augments': 'tft-augments.json',
            'traits': 'tft-trait.json'
        }
        for key, value in dd_endpoints.items():
            url = base_url + value
            data = fetch_data_from_DD(url)
            buffer = format_data(key, data)
            upload_DD_data_to_gcs(
                buffer, BUCKET, f'{key}.parquet', BUCKET_SUBDIR_DD
            )

    @task()
    def dd_data_gcs_to_bq():
        """ Upload Data Dragon (DD) data to BigQuery """
        upload_dd_data_to_bigquery(
            PROJECT_ID,
            BUCKET, 
            BUCKET_SUBDIR_DD, 
            BQ_DATASET, 
            DD_ID_TO_NAME_TABLES
        )

    #-----------------------Preparing the environment-------------------------
    @task()
    def check_patch():
        return get_current_patch()
        
    @task()
    def delete_old_resources_if_new_patch(curr_patch: str):   
        new = check_patch_difference(curr_patch)
        if new: 
            delete_gcs_files(BUCKET, f'{BUCKET_SUBDIR}/')
            delete_bq_tables(PROJECT_ID, BQ_DATASET)

    #-------------------------Ingesting Match Data----------------------------
    @task()
    def upload_matches_to_gcs(curr_patch: str):
        """ Get match details and upload the parquetized files to GCS. """
        api = configure_api()
        summoner_ids = get_summonerIds(api)
        puuids = get_puuids(api, summoner_ids)
        all_matches = get_unique_match_ids(api, puuids)
        fetch_and_upload_match_details(
            api, all_matches, curr_patch, BUCKET, BUCKET_SUBDIR
        )

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
        dataproc_jars = [
            "gs://spark-lib/bigquery/spark-3.1-bigquery-0.27.0-preview.jar"
        ]
    )

    delete_dataproc_cluster_task = DataprocDeleteClusterOperator(
        task_id='delete_dataproc_cluster_task',
        project_id=PROJECT_ID,
        cluster_name=CLUSTER_NAME,
        region=CLUSTER_REGION,
        trigger_rule='all_done'
    )


    check_patch_task = check_patch()
    delete_previous_patch_data_task = (
        delete_old_resources_if_new_patch(curr_patch=check_patch_task)
    )
    upload_matches_task = upload_matches_to_gcs(curr_patch=check_patch_task)
    upload_id_to_name_mapping_task = upload_id_to_name_mapping()
    dd_data_gcs_to_bq_task = dd_data_gcs_to_bq()


    upload_id_to_name_mapping_task >> dd_data_gcs_to_bq_task

    (check_patch_task 
     >> delete_previous_patch_data_task
     >> upload_matches_task 
     >> create_dataproc_cluster_task 
     >> submit_spark_job_task 
     >> delete_dataproc_cluster_task)

dag = ingestion_dag()
