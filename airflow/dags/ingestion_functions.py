import os
import re
import json
import pyarrow as pa
import pyarrow.parquet as pq

from riotwatcher import TftWatcher, ApiError
from dotenv import load_dotenv
from pandas import json_normalize
from google.cloud import storage
from urllib.request import urlopen

REGION = 'NA1'

def get_current_patch() -> str:
    """ Obtains the latest patch version from Riot Data Dragon """
    url = 'https://ddragon.leagueoflegends.com/api/versions.json'
    response = urlopen(url)
    data_json = json.loads(response.read())
    return data_json[0]


def parquetize_json(obj: dict) -> pa.Table:
    df = json_normalize(obj)
    table = pa.Table.from_pandas(df)
    return table

def upload_to_gcs(table: pa.Table, bucket: str, file_name: str) -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(file_name)

    with blob.open('wb') as f:
        pq.write_table(table, f)


def configure_api() -> TftWatcher:
    load_dotenv()
    api_key = os.getenv('API_KEY')
    tft_watcher = TftWatcher(api_key)
    return tft_watcher

def get_summonerIds(tft_watcher: TftWatcher) -> list:
    """
    Retrieves account IDs for players in the 2 higest ranks in TFT.
    Summoner IDs are only unique per region.
    """
    summoner_ids = []

    ranks = {
        'challenger': tft_watcher.league.challenger(region=REGION),
        'grandmaster': tft_watcher.league.grandmaster(region=REGION),
        #'master': tft_watcher.league.master(region=REGION) # Uncomment this line to get master rank data
    }

    summoner_ids = [
        summoner['summonerId'] 
        for rank in ranks 
        for summoner in ranks[rank]['entries']
    ]
    return summoner_ids[:3] # Remove [:3] to get all summonerIds

def get_puuids(tft_watcher: TftWatcher, summoner_ids: list) -> list:
    """
    Retrieves account IDs for players within a list of Summoner IDs.
    PUUIDs are unique globally, and are required to get match IDs.
    """
    puuids = []

    for summoner_id in summoner_ids:
        puuids.append(tft_watcher.summoner.by_id(REGION, summoner_id)['puuid'])
    return puuids

def get_unique_match_ids(tft_watcher: TftWatcher, puuids: list) -> list:
    match_ids = {match 
                 for puuid in puuids 
                 for match in tft_watcher.match.by_puuid(REGION, puuid)}
    return list(match_ids)

def get_patch_of_match(match: dict) -> str:
    game_version = match['info']['game_version']
    pattern = r"(\d+\.\d+)"
    patch_match = re.search(pattern, game_version)
    patch = patch_match.group(1) + '.1' if patch_match else None
    return patch


def fetch_and_upload_match_details(
    tft_watcher: TftWatcher, match_ids: list, curr_patch: str, bucket: str
) -> None:
    for match_id in match_ids:
        # Get match details (match end-state summary information)
        match = tft_watcher.match.by_id(REGION, match_id)
        queue_id = match['info']['queue_id']
        patch = get_patch_of_match(match)

        # Filtering for queue_id == 1100 as it is the queue_id for ranked TFT
        if queue_id == 1100 and patch == curr_patch:
            match_pq = parquetize_json(match)
            file_name = f'{match_id}.parquet'
            upload_to_gcs(match_pq, bucket, file_name)
