import io
import json
import pandas as pd

from google.cloud import storage, bigquery
from urllib.request import urlopen

def upload_DD_data_to_gcs(
        pq: io.BytesIO, bucket: str, file_name: str, folder_name: str
    ) -> None:
    client = storage.Client()
    bucket = client.get_bucket(bucket)
    blob = bucket.blob(f"{folder_name}/{file_name}")

    pq.seek(0)
    blob.upload_from_file(pq, content_type='application/octet-stream')

def fetch_data_from_DD(url: str) -> dict:
    """ Maps ids to names from a given url using Riot Data Dragon """
    response = urlopen(url)
    data_json = json.loads(response.read())
    extracted_data = [
        {
            "id": value["id"], "name": value["name"]
        } for key, value in data_json["data"].items()
    ]
    return extracted_data

def format_item_data(df_items: pd.DataFrame) -> pd.DataFrame:
    # Manually listing the support items as they are not categorized in DD.
    support_items = [
        'Aegis of the Legion',
        'Zephyr',
        "Zeke's Herald",
        'Locket of the Iron Solari',
        'Shroud of Stillness',
        'Obsidian cleaver',
        'Chalice of Power',
        "Bashee's Veil",
        'Virtue of the Martyr',
        "Randuin's Omen",
        "Zz'Rot Portal",
        'Needlessly Big Gem'
    ]
    
    df_items['prefix'] = df_items['id'].str.split('_').str[:2].str.join('_')
    df_items['item_type'] = df_items.apply(
        lambda row: 
            'Support Item' if row['name'] in support_items 
            else 'Normal Item' if row['prefix'] == 'TFT_Item' 
            else 'Radiant Item' if row['prefix'] == 'TFT5_Item' 
            else 'Shimmerscale Item' if row['prefix'] == 'TFT7_Item' 
            else 'Ornn Item' if row['prefix'] in ['TFT4_Item', 'TFT9_Item'] 
            else 'Emblem' if row['prefix'] == 'TFT10_Item' 
            else 'Other', axis=1
    )
    df_items.drop(columns=['prefix'], inplace=True)
    return df_items

def format_data(dd_endpoint: str, data: dict) -> io.BytesIO:
    df = pd.DataFrame(data)
    if dd_endpoint == 'items':
        df = format_item_data(df)

    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    return buffer


def upload_dd_data_to_bigquery(
    project_id: str, bucket: str, folder: str, dataset: str, tables: dict
) -> None:
    client = bigquery.Client(project=project_id)
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.PARQUET,
        write_disposition='WRITE_TRUNCATE',
        autodetect=True
    )

    for dd_endpoint, id in tables.items():
        gcs_uri = f'gs://{bucket}/{folder}/{dd_endpoint}.parquet'
        table_id = f'{dataset}.{id}'
        load_job = client.load_table_from_uri(
            gcs_uri, table_id, job_config=job_config
        )
        load_job.result()
        if load_job.state == 'DONE':
            print(f'Successfully loaded {dd_endpoint} data into {table_id}')
        else:
            print('Error loading data into BigQuery:', load_job.errors)
