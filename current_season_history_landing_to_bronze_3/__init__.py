import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import BytesIO
import logging
from deltalake import write_deltalake
from azure.identity import DefaultAzureCredential
import pyarrow as pa
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import polars as pl


def read_file_from_adls(directory_client, file_name):
    file_client = directory_client.get_file_client(file_name)
    download = file_client.download_file()
    downloaded_bytes = download.readall() # Output is in raw bytes
    logging.info("File is read from ADLS")
    return pd.read_json(BytesIO(downloaded_bytes))


def read_file_from_adls_using_polars(credential, data_source, ingest_date):
    container_name = os.getenv("StorageAccountContainer")
    StorageAccountName = os.getenv("StorageAccountName")
    azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/landing/{data_source}/current/{ingest_date}/"
    df = pl.read_ndjson(azure_path, storage_options=credential)

    return df


def handle_player_metadata_column(dataset):
    try:
        columns_to_fix = ['expected_goals_per_90', 'saves_per_90', 'expected_assists_per_90', 'expected_goal_involvements_per_90', 'expected_goals_conceded_per_90', 'goals_conceded_per_90', 'starts_per_90', 'clean_sheets_per_90']
        # Handle the inconsistent columns
        player_metadata_fixed_dataset = handle_inconsistent_columns(dataset, columns_to_fix)
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")

    return player_metadata_fixed_dataset



def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, data_source):
    try:
        dataset.write_delta(
            f"abfss://{container_name}@{adls_url}/bronze/{data_source}",
            mode="overwrite",
            delta_write_options={"schema_mode": "overwrite"},
            storage_options=storage_options
        )
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def add_load_date_column(football_dataframe, ingest_date):
    df = football_dataframe.with_columns([
        pl.lit(ingest_date).alias('ingest_date')
    ])
    logging.info(f"Ingest date column with value {ingest_date} has been added")
    
    return df


def handle_inconsistent_columns(df, columns_to_fix):
    for column in columns_to_fix:
        try:
            # Try to convert to float, if fails, keep as is
            df = df.with_columns([
                pl.col(column).fill_null(0).cast(pl.Float64).alias(column)  # Cast column to float64
            ])
            logging.info(f"Converted column {column} to float")
        except Exception as e:
            # If conversion fails, we'll keep the column as is and log a warning
            logging.warning(f"Could not convert column {column} to float64. Keeping original data type. Error: {e}")
    return df


def handle_inconsistent_columns_new(df):
    for column in df.columns:
        try:
            # Replace Nulls with 'null' and cast to string (Utf8)
            df = df.with_columns([
                pl.col(column).fill_null("null").cast(pl.Utf8).alias(column)
            ])
            logging.info(f"Converted column {column} to string")
        except Exception as e:
            # If conversion fails, log a warning and keep the original data type
            logging.warning(f"Could not convert column {column} to string. Keeping original data type. Error: {e}")
    return df


def check_data_types(df):
    data_types = df.dtypes.to_dict()
    logging.info("Data types after handling inconsistent columns:")
    for column, dtype in data_types.items():
        logging.info(f"{column}: {dtype}")
    return data_types


def get_json_column_list(df):
    return df.columns


def get_landing_column_list(credential, layer, data_source):
    try:
        container_name = os.getenv("StorageAccountContainer")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
        logging.info(f"Reading {azure_path}")
        dataset = pl.read_delta(azure_path, storage_options=credential)
        landing_column_list = dataset.columns
        item_to_remove = {'ingest_date'}
        landing_column_list = list(filter(lambda x: x not in item_to_remove, landing_column_list))
        logging.info(f"Removed column: {item_to_remove}")
        return landing_column_list
    except Exception as e:
        logging.error(f"Removed column fails: {str(e)}")
    


def compare_columns(landing_column_list, json_column_list):
    landing_set = set(landing_column_list)
    json_set = set(json_column_list)
    if sorted(landing_column_list) == sorted(json_column_list):
        logging.info("All columns match")
    else:
        missing_in_json = landing_set - json_set
        missing_in_landing = json_set - landing_set
        if missing_in_json:
            logging.error(f"Columns missing in JSON: {missing_in_json}")
        if missing_in_landing:
            logging.error(f"Columns missing in landing: {missing_in_landing}")


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    data_source_type = req.params.get('data_source')
    file_date = req.params.get('file_date')

    if not data_source_type:
        return func.HttpResponse(
            "No parameter supplied. Please provide a 'data_source' parameter. Input could be either current_season_history or player_metadata or team_metadata or position_metadata",
            status_code=400
        )
    elif not file_date:
        return func.HttpResponse(
            "No parameter supplied. Please provide a 'file_date' based on the file to be ingested. Date format should be ddMMyyyy",
            status_code=400
        )

    try:
        container_name = os.getenv("StorageAccountContainer")
        adls_url_v2 = os.getenv("DataLakeUrllll")
        password = create_storage_options(os.getenv('KeyVault'))
        x = read_file_from_adls_using_polars(password, data_source_type, file_date)
        json_column_list = get_json_column_list(x)
        bronze_column_list = get_landing_column_list(password, 'bronze', data_source_type)
        compare_columns(bronze_column_list, json_column_list)
        current_season_dataset_new = add_load_date_column(x, file_date)
        if data_source_type == "player_metadata":
            player_metadata_dataset = handle_inconsistent_columns_new(current_season_dataset_new)
            write_raw_to_bronze(player_metadata_dataset, password, container_name, adls_url_v2, data_source_type)
        else:
            player_metadata_dataset = handle_inconsistent_columns_new(current_season_dataset_new)
            write_raw_to_bronze(player_metadata_dataset, password, container_name, adls_url_v2, data_source_type)
    
        return func.HttpResponse(f"Data has been uploaded into bronze layer for data source - {bronze_column_list}", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)