from datetime import datetime, timedelta
from airflow.decorators import dag, task
from ingestion_functions import *

PROJECT_ID = os.getenv('GCP_PROJECT_ID')
BUCKET = os.getenv('GCP_GCS_BUCKET')

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
        """ A simple task to return the current patch version """
        return get_current_patch()
    
    @task()
    def upload_matches_to_gcs(curr_patch: str):
        """ 
        A task to upload match details to GCS.
        This task first fetches match summary information for grandmaster+ 
        ranked players in North America. It then directly uploads the info 
        for each match to GCS (in parquet format).
        """
        api = configure_api()
        summoner_ids = get_summonerIds(api)
        puuids = get_puuids(api, summoner_ids)
        all_matches = get_unique_match_ids(api, puuids)
        fetch_and_upload_match_details(api, all_matches, curr_patch, BUCKET)

    curr_patch = check_patch()
    upload_matches_to_gcs(curr_patch=curr_patch)

dag = ingestion_dag()
