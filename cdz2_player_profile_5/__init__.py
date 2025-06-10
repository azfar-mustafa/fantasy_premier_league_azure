import logging
import os
import polars as pl
import azure.functions as func
from util.common_func import create_storage_options
from datetime import datetime


def read_data(credential, ingest_date, data_source, layer):
    container_name = os.getenv("StorageAccountContainer")
    StorageAccountName = os.getenv("StorageAccountName")
    azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
    logging.info(f"Reading {azure_path}")
    dataset = pl.read_delta(azure_path, storage_options=credential)
    data_df = dataset.filter(pl.col("ingest_date") == ingest_date)
    logging.info(f"Created {data_source} dataset for ingest date = {ingest_date}")
    return data_df


def create_season_value(file_date):
    year = int(file_date[4:])
    month = int(file_date[2:4])

    month_a_list = [8,9,10,11,12]

    if month in month_a_list:
        year_a = int(year) + 1
        actual_season = str(year) + '/' + str(year_a)
        logging.info(f"Actual season: {actual_season}")
        return actual_season
    else:
        actual_year = int(year) - 1
        actual_season = str(actual_year) + '/' + str(year)
        logging.info(f"Actual season: {actual_season}")
        return actual_season
    

def add_season_to_dataset(dataset, season_year):
    #season = create_season_value(season_year)
    
    # Convert to Polars DataFrame if it isn't already
    if not isinstance(dataset, pl.DataFrame):
        dataset = pl.from_arrow(dataset)
    # Add new columns
    result = dataset.with_columns([
        pl.lit(season_year).alias('season'),
        pl.lit(datetime.now()).dt.strftime('%Y-%m-%d %H:%M:%S').alias('inserted_timestamp'),
        pl.lit(datetime.now()).dt.strftime('%Y-%m-%d %H:%M:%S').alias('updated_timestamp')
    ])
    
    logging.info(f"Add season column - {season_year} to dataset")
    
    return result


def join_multiple_dataset(dataset_1, dataset_2, dataset_3):
    test_1 = dataset_1
    test_2 = dataset_2
    test_3 = dataset_3
    result = test_1.join(test_2, left_on='element_type', right_on='id', how='left')
    result_new = result.join(test_3, left_on='team', right_on='id', how='left')
    result_latest = result_new.drop(["element_type", "team"])
    return result_latest


def data_quality(dataset_4):
    dtype_mapping = {
        'id': pl.String,
        'web_name': pl.String,
        'name': pl.String,
        'singular_name': pl.String,
        'now_cost': pl.Float64,
        'status': pl.String,
        'can_select': pl.String,
        'birth_date': pl.Datetime,
        'team_join_date': pl.Datetime,
        'season': pl.String,
        'inserted_timestamp': pl.Datetime,
        'updated_timestamp': pl.Datetime
    }

    datetime_cols = {'birth_date', 'team_join_date', 'inserted_timestamp', 'updated_timestamp'}

    df = dataset_4.with_columns([
        (
            pl.col(col_name)
            .replace("null", None)
            .str.strptime(pl.Datetime, "%Y-%m-%d %H:%M:%S", strict=False)
            if col_name in datetime_cols else
            pl.col(col_name)
            .replace("null", None)
            .cast(dtype)
        ).alias(col_name)
        for col_name, dtype in dtype_mapping.items()
    ])

    logging.info(f"Converted column datatype for current_season_history dataset")

    return df


def write_bronze_to_silver(dataset, storage_options, container_name, adls_url, layer, data_source):
    try:
        #write_deltalake(f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/{data_source}", dataset, storage_options=storage_options, mode='overwrite', engine='rust', schema_mode='overwrite', schema=column)
        dataset.write_delta(
            f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/{data_source}",
            mode="append",
            storage_options=storage_options
        )
        logging.info(f"Dataset has been inserted into {layer} layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    try:
        adls_url = os.getenv("StorageAccountName")
        container_name = os.getenv("StorageAccountContainer")
        silver_layer = 'silver'
        data_source_type = 'cdz2_player_profile'
        ingest_date = '17022025'
        password = create_storage_options(os.getenv('KeyVault'))
        season = create_season_value(ingest_date)
        df_player_metadata = read_data(password, ingest_date, data_source='player_metadata', layer='bronze')
        df_player_metadata_select = df_player_metadata.select(["id", "element_type", "team", "web_name", "now_cost", "status", "can_select", "birth_date", "team_join_date"])
        df_position_metadata = read_data(password, ingest_date, data_source='position_metadata', layer='bronze')
        df_position_metadata_select = df_position_metadata.select(["id", "singular_name"])
        df_team_metadata = read_data(password, ingest_date, data_source='team_metadata', layer='bronze')
        df_team_metadata_select = df_team_metadata.select(["id", "name"])
        latest_dataset = join_multiple_dataset(df_player_metadata_select, df_position_metadata_select, df_team_metadata_select)
        latest_dataset_2 = add_season_to_dataset(latest_dataset, season)
        latest_dataset_3 = data_quality(latest_dataset_2)
        df_reordered = latest_dataset_3.select(["id", "web_name", "name", "singular_name", "now_cost", "status", "can_select", "birth_date", "team_join_date", "season", "inserted_timestamp", "updated_timestamp"])
        write_bronze_to_silver(df_reordered, password, container_name, adls_url, silver_layer, data_source_type)
        return func.HttpResponse(f"Process Completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)