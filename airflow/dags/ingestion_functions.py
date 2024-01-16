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
    url = 'https://ddragon.leagueoflegends.com/api/versions.json'
    response = urlopen(url)
    data_json = json.loads(response.read())
    return data_json[0]


def configure_api() -> TftWatcher:
    load_dotenv()
    api_key = os.getenv('API_KEY')
    tft_watcher = TftWatcher(api_key)
    return tft_watcher

def get_summonerIds(tft_watcher: TftWatcher) -> dict:
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
    puuids = []

    for summoner_id in summoner_ids:
        puuids.append(tft_watcher.summoner.by_id(REGION, summoner_id)['puuid'])
    return puuids

def get_matches(tft_watcher: TftWatcher, puuids: list) -> list:
    match_ids = {match 
                 for puuid in puuids 
                 for match in tft_watcher.match.by_puuid(REGION, puuid)}
    return list(match_ids)

# def get_match_details(tft_watcher: TftWatcher, match_ids: list) -> list:
#     matches = [tft_watcher.match.by_id(REGION, match_id) for match_id in match_ids]
#     # Filtering for queue_id == 1100 as it is the queue_id for ranked TFT
#     return [match for match in matches if match['info']['queue_id'] == 1100]

def upload_match_to_gcs(match: dict, bucket: str, file_name: str) -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(file_name)
    blob.upload_from_string(json.dumps(match), 'application/json')

def upload_match_details(
    tft_watcher: TftWatcher, match_ids: list, curr_patch:str, bucket: str
) -> None:
    for match_id in match_ids:
        match = tft_watcher.match.by_id(REGION, match_id)
        queue_id = match['info']['queue_id']
        game_version = match['info']['game_version']
        pattern = r"(\d+\.\d+)"
        patch_match = re.search(pattern, game_version)
        patch = patch_match.group(1) + '.1' if patch_match else None

        # Filtering for queue_id == 1100 as it is the queue_id for ranked TFT
        if queue_id == 1100 and patch == curr_patch:
            file_name = f'{match_id}.json'
            upload_match_to_gcs(match, bucket, file_name)
