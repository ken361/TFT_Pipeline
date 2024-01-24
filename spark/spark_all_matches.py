import sys
from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark import SparkContext
from pyspark.sql import functions as F
from pyspark.sql.functions import col, expr, regexp_replace
from pyspark.sql.types import StringType, ArrayType


def remove_leading_text_string(input):
    return expr(f"substring_index({input}, '_', -1)")

def remove_leading_text_array(input):
    return expr(f"transform({input}, x -> substring_index(x, '_', -1))")
    
def remove_leading_text_nested(input):
    nested_expr = f"transform({input}, x -> {remove_leading_text_array('x')})"
    return expr(nested_expr)

def remove_tft_item_prefix(df, column_name):
    remove_prefix_udf = (
        expr(
            "transform({}, arr -> transform(arr, item -> "
            "substring_index(item, '_', -1)"
            "))".format(column_name)
            )
        )
    df = df.withColumn(column_name, remove_prefix_udf)
    return df

def convert_to_title_case(df, column_name):
    title_case_udf = (
        expr(
            "transform({}, arr -> transform(arr, item -> "
            "regexp_replace(item, '([a-z0-9])([A-Z])', '$1 $2')"
            "))".format(column_name)
            )
    )
    df = df.withColumn(column_name, title_case_udf)
    return df


GCP_PROJECT_ID = sys.argv[1]
BQ_DATASET     = sys.argv[2]
BQ_TABLE       = sys.argv[3]
TFT_UNIT_TABLE = sys.argv[4]
BUCKET         = sys.argv[5]

def main():

    conf = SparkConf() \
        .setAppName('tft-gcp-dataproc') \
        .set('spark.hadoop.google.cloud.auth.service.account.enable', 'true')

    sc = SparkContext(conf=conf)

    spark = SparkSession.builder \
        .config(conf=sc.getConf()) \
        .getOrCreate()


    df_raw = (
        spark.read.option('mergeSchema', 'true')
        .parquet(f'gs://{BUCKET}/*')
    )


    df_list = []

    for i in range(8):
        metadata_col = col('`metadata.match_id`')
        participant_col = col('`info.participants`')

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

    df_all_matches = df_list[0]
    for df_i in df_list[1:]:
        df_all_matches = df_all_matches.union(df_i)


    df_unit_rarity = df_all_matches \
        .select('unit_name', 'unit_rarity') \
        .distinct()

    df_all_matches = df_all_matches.drop('unit_rarity')


    list_fields_to_transform = ['augments', 'trait_names', 'unit_name']
    nested_fields_to_transform = ['unit_items']

    for field in list_fields_to_transform:
        df_all_matches = df_all_matches \
            .withColumnRenamed(field, f'{field}_raw') \
            .withColumn(field, remove_leading_text_array(f'{field}_raw')) \
            .drop(f'{field}_raw')

    for field in nested_fields_to_transform:
        df_all_matches = remove_tft_item_prefix(df_all_matches, field)
        df_all_matches = convert_to_title_case(df_all_matches, field)


    df_all_matches.write \
    .format('bigquery') \
    .option('table', f'{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE}') \
    .mode("overwrite") \
    .save()

    df_unit_rarity.write \
    .format('bigquery') \
    .option('table', f'{GCP_PROJECT_ID}.{BQ_DATASET}.{TFT_UNIT_TABLE}') \
    .mode("overwrite") \
    .save()


if __name__ == "__main__":
    main()
