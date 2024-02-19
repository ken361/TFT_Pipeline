import os
import re
import json
import time
from datetime import datetime
from urllib.request import urlopen

import pyarrow as pa
import pyarrow.parquet as pq
from pandas import json_normalize

from riotwatcher import TftWatcher, ApiError
from dotenv import load_dotenv
from google.cloud import storage, bigquery

REGION = 'NA1'


#----------------------Ingesting & Uploading Match Data-----------------------
def configure_api() -> TftWatcher:
    load_dotenv()
    api_key = os.getenv('API_KEY')
    tft_watcher = TftWatcher(api_key)
    return tft_watcher


def get_summonerIds(tft_watcher: TftWatcher) -> list:
    """
    Retrieves account IDs for players in the higest ranks in TFT.
    Summoner IDs are only unique per region.
    """
    summoner_ids = []

    ranks = {
        'challenger': tft_watcher.league.challenger(region=REGION),
        'grandmaster': tft_watcher.league.grandmaster(region=REGION),
        #'master': tft_watcher.league.master(region=REGION)
    }

    summoner_ids = [
        summoner['summonerId'] 
        for rank in ranks 
        for summoner in ranks[rank]['entries']
    ]
    return summoner_ids


def get_puuids(tft_watcher: TftWatcher, summoner_ids: list) -> list:
    """
    Retrieves account IDs for players within a list of Summoner IDs.
    PUUIDs are unique globally, and are required to get match IDs.
    """
    puuids = []

    for summoner_id in summoner_ids:
        puuids.append(
            tft_watcher.summoner.by_id(REGION, summoner_id)['puuid']
        )
    return puuids


def get_yesterday_start_end_epoch() -> list:
    # Every day, dag runs at midnight, so we want to get the matches 
    # from the previous day. We can do this by using epoch time.
    current_time_seconds = time.time()
    yesterday_time_seconds = current_time_seconds - 86400
    yesterday_time_struct = time.localtime(yesterday_time_seconds)
    yesterday_midnight_struct = time.struct_time((
        yesterday_time_struct.tm_year, 
        yesterday_time_struct.tm_mon, 
        yesterday_time_struct.tm_mday, 
        0, 0, 0, 
        yesterday_time_struct.tm_wday, 
        yesterday_time_struct.tm_yday, 
        yesterday_time_struct.tm_isdst
    ))
    yday_midnight_epoch_time = int(time.mktime(yesterday_midnight_struct))
    tday_midnight_epoch_time = yday_midnight_epoch_time + 86399
    return [yday_midnight_epoch_time, tday_midnight_epoch_time]


def get_unique_match_ids(tft_watcher: TftWatcher, puuids: list) -> list:
    yesterday_start_end_epoch = get_yesterday_start_end_epoch()
    # Comment out 'start_time' to get past 20 matches per player
    match_ids = {match 
                 for puuid in puuids 
                 for match in tft_watcher.match.by_puuid(
                     region=REGION, 
                     puuid=puuid, 
                     start_time=yesterday_start_end_epoch[0],
                     end_time=yesterday_start_end_epoch[1])}
    return list(match_ids)


def get_patch_of_match(match: dict) -> str:
    game_version = match['info']['game_version']
    pattern = r"(\d+\.\d+)"
    patch_match = re.search(pattern, game_version)
    patch = patch_match.group(1) + '.1' if patch_match else None
    return patch


def parquetize_json(obj: dict) -> pa.Table:
    df = json_normalize(obj)
    table = pa.Table.from_pandas(df)
    return table


def upload_to_gcs(
        table: pa.Table, bucket: str, file_name: str, folder_name: str
) -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(f"{folder_name}/{file_name}")
    with blob.open('wb') as f:
        pq.write_table(table, f)


def fetch_and_upload_match_details(
    tft_watcher: TftWatcher,
    match_ids: list, 
    curr_patch: str, 
    bucket: str, 
    folder_name: str
) -> None:
    today_date_code = datetime.now().strftime('%y%m%d')
    for match_id in match_ids:
        # Get match details (match end-state summary information)
        try:
            match = tft_watcher.match.by_id(REGION, match_id)
        except ApiError as e:
            if e.response.status_code == 429 or e.response.status_code == 500:
                retry_after = e.response.headers.get('Retry-After')
                # This retry-after is handled by the RiotWatcher library
                print(f'Error, retrying in {retry_after} seconds.')
            elif e.response.status_code == 403:
                continue # Some matches randomly don't seem to be accessible
            else:
                raise
        queue_id = match['info']['queue_id']
        patch = get_patch_of_match(match)

        # Filtering for queue_id == 1100 as it is the queue_id for ranked TFT
        if queue_id == 1100 and patch == curr_patch:
            match_pq = parquetize_json(match)
            file_name = f'{match_id}-{today_date_code}.parquet'
            upload_to_gcs(match_pq, bucket, file_name, folder_name)


#-----------------------Preparing the environment-------------------------
def get_current_patch() -> str:
    """ Obtains the latest patch version from Riot Data Dragon """
    url = 'https://ddragon.leagueoflegends.com/api/versions.json'
    response = urlopen(url)
    data_json = json.loads(response.read())
    return data_json[0]


# Was sometimes getting errors reading and writing with mode 'r+'
def check_patch_difference(curr_patch: str):
    # Simple, but probably temporary measure to store the patch value
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    file_path = os.path.join(curr_dir, 'tft_patch.txt')
    
    with open(file_path, 'r') as f:
        old_patch = f.read()

    if old_patch != curr_patch:
        with open(file_path, 'w') as f:
            f.write(curr_patch)
            return True
    else: 
        return False


def delete_gcs_files(bucket_name: str, prefix: str) -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket_name)
    blobs = bucket.list_blobs(prefix=prefix)
    for blob in blobs:
        blob.delete()


def delete_bq_tables(project_id: str, dataset: str) -> None:
    client = bigquery.Client(project=project_id)
    tables = client.list_tables(dataset)
    for table in tables:
        client.delete_table(f'{project_id}.{dataset}.{table.table_id}')
