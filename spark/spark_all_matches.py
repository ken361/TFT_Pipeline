import sys
from datetime import datetime
from pyspark.sql import SparkSession, DataFrame
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import functions as F
from functools import reduce

#  Quite a few of the functions in this file have been removed due to a
#  change in implementation of the pipeline. At the bottom of this file
#  is an example of a function that was used, and the usage of such functions.


GCP_PROJECT_ID    = sys.argv[1]
BQ_DATASET        = sys.argv[2]
UNITS_ALL_TABLE   = sys.argv[3]
UNIT_RARITY_TABLE = sys.argv[4]
TRAITS_TABLE      = sys.argv[5]
AUGMENTS_TABLE    = sys.argv[6]
BUCKET            = sys.argv[7]
BUCKET_SUBDIR     = sys.argv[8]
TEMP_BUCKET       = sys.argv[9]

def main():

    conf = SparkConf() \
        .setAppName('tft-gcp-dataproc') \
        .set('spark.hadoop.google.cloud.auth.service.account.enable', 'true')

    sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()


    current_date = datetime.now().strftime("%y%m%d")
    file_path = f'gs://{BUCKET}/{BUCKET_SUBDIR}/*-{current_date}.parquet'
    df_raw = (
        spark.read.parquet(file_path)
        .withColumn(
            "`info.game_length`", F.col("`info.game_length`").cast("double")
        )
    )


    df_list = []
    for i in range(8):
        metadata_col = F.col('`metadata.match_id`')
        participant_col = F.col('`info.participants`')

        df_i = df_raw.select(
            metadata_col.alias('match_id'),
            participant_col
                .getItem('placement').getItem(i)
                .alias('placement'),
            participant_col
                .getItem('augments').getItem(i)
                .alias('augments'),
            participant_col
                .getItem('traits').getItem(i).getItem('name')
                .alias('trait_names'),
            participant_col
                .getItem('traits').getItem(i).getItem('style')
                .alias('trait_styles'),
            participant_col
                .getItem('units').getItem(i).getItem('character_id')
                .alias('unit_name'),
            participant_col
                .getItem('units').getItem(i).getItem('rarity')
                .alias('unit_rarity'),
            participant_col
                .getItem('units').getItem(i).getItem('tier')
                .alias('unit_tier'),
            participant_col
                .getItem('units').getItem(i).getItem('itemNames')
                .alias('unit_items'),
        )
        df_list.append(df_i)

    df_all_matches = reduce(DataFrame.union, df_list)

    # Trait table
    df_trait = df_all_matches \
        .select('match_id', 'placement', 'trait_names', 'trait_styles')

    # Augment table
    df_augment = df_all_matches \
        .select('match_id', 'placement', 'augments')

    # Unit/champion table
    df_champs_all = (
        df_all_matches
        .withColumn(
            'unit', 
            F.arrays_zip('unit_name', 'unit_tier', 'unit_rarity', 'unit_items')
        ) \
        .withColumn('unit', F.explode('unit')) \
        .select('match_id', 
                'placement', 
                F.col('unit.unit_name').alias('unit_name'),
                F.col('unit.unit_tier').alias('unit_tier'),
                F.col('unit.unit_rarity').alias('unit_rarity'),
                F.col('unit.unit_items').alias('unit_items')
        )
    )

    # Unit/champion rarity table
    df_champ_rarity = df_champs_all \
        .select('unit_name', 'unit_rarity') \
        .distinct()


    # Write to BigQuery
    write_options = {
        'format': 'com.google.cloud.spark.bigquery.BigQueryRelationProvider',
        'temporaryGcsBucket': TEMP_BUCKET,
        'createDisposition': 'CREATE_IF_NEEDED'
    }

    df_champs_all.drop('unit_rarity').write.mode('append').options(
        **write_options, 
        table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.{UNITS_ALL_TABLE}'
    ).save()

    df_trait.write.mode('append').options(
        **write_options, 
        table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.{TRAITS_TABLE}'
    ).save()

    df_augment.write.mode('append').options(
        **write_options, 
        table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.{AUGMENTS_TABLE}'
    ).save()

    df_champ_rarity.write.mode('overwrite').options(
        **write_options, 
        table=f'{GCP_PROJECT_ID}.{BQ_DATASET}.{UNIT_RARITY_TABLE}'
    ).save()


if __name__ == "__main__":
    main()

# def convert_str_to_title_case(input):
#     title_case_udf = F.expr(
#         f"transform({input}, x -> "
#         "regexp_replace(x, '([a-z])([0-9])|([0-9])([a-z])|([a-z])([A-Z])'"
#         ", '$1$3$5 $2$4$6'))"
#     )
#     return title_case_udf


# for field in list_fields_to_transform:
#     df_all_matches = df_all_matches \
#         .withColumnRenamed(field, f'{field}_raw') \
#         .withColumn(field, remove_leading_text_array(f'{field}_raw')) \
#         .withColumn(field, convert_str_to_title_case(f'{field}_raw')) \
#         .drop(f'{field}_raw')

# for field in nested_fields_to_transform:
#     df_all_matches = remove_tft_item_prefix(df_all_matches, field)
#     df_all_matches = convert_items_to_title_case(df_all_matches, field)
