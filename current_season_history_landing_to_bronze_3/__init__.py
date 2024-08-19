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


def read_file_from_adls(directory_client, file_name):
    file_client = directory_client.get_file_client(file_name)
    download = file_client.download_file()
    downloaded_bytes = download.readall() # Output is in raw bytes
    logging.info("File is read from ADLS")
    return pd.read_json(BytesIO(downloaded_bytes))


def handle_player_metadata_column(dataset):
    try:
        columns_to_fix = ['expected_goals_per_90', 'saves_per_90', 'expected_assists_per_90', 'expected_goal_involvements_per_90', 'expected_goals_conceded_per_90', 'goals_conceded_per_90', 'starts_per_90', 'clean_sheets_per_90']
        # Handle the inconsistent columns
        player_metadata_fixed_dataset = handle_inconsistent_columns(dataset, columns_to_fix)
        # Convert pandas DataFrame to PyArrow Table
        #player_metadata_fixed_dataset_pyarrow = pa.Table.from_pandas(player_metadata_fixed_dataset)
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")

    return player_metadata_fixed_dataset



def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, data_source):
    try:
        pyarrow_dataset = pa.Table.from_pandas(dataset)
        write_deltalake(f"abfss://{container_name}@{adls_url}/bronze/{data_source}", pyarrow_dataset, storage_options=storage_options, mode='append', schema_mode='merge', engine='rust')
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def add_load_date_column(football_dataframe, ingest_date):
    football_dataframe['ingest_date'] = ingest_date
    logging.info(f"Ingest date column with value {ingest_date} has been added")
    return football_dataframe


def handle_inconsistent_columns(df, columns_to_fix):
    for column in columns_to_fix:
        # Try to convert to float64, if fails, keep as is
        try:
            df[column] = df[column].astype('float64')
            logging.info(f"Converted column {column} to float")
        except ValueError:
            # If conversion fails, we'll keep the column as is and log a warning
            logging.warning(f"Could not convert column {column} to float64. Keeping original data type.")
    return df


def check_data_types(df):
    data_types = df.dtypes.to_dict()
    logging.info("Data types after handling inconsistent columns:")
    for column, dtype in data_types.items():
        logging.info(f"{column}: {dtype}")
    return data_types


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    data_source_type = req.params.get('data_source')

    if not data_source_type:
        return func.HttpResponse(
            "No parameter supplied. Please provide a 'data_source' parameter. Input could be either current_season_history or player_metadata or team_metadata or position_metadata",
            status_code=400
        )

    try:
        container_name = os.getenv("StorageAccountContainer")
        adls_url = os.getenv("DataLakeUrl")
        adls_url_v2 = os.getenv("DataLakeUrllll")
        current_date = convert_timestamp_to_myt_date()
        #current_date = '05072024'
        file_name = f"{data_source_type}_{current_date}.json"
        azure_dev_key_vault_url = os.getenv("KeyVault")
        credentials = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url=adls_url, credential=credentials)
        password = create_storage_options(azure_dev_key_vault_url)
        directory_client = service_client.get_file_system_client(container_name).get_directory_client(f"landing/{data_source_type}/current/{current_date}")
        current_season_dataset = read_file_from_adls(directory_client, file_name)
        current_season_dataset_new = add_load_date_column(current_season_dataset, current_date)
        if data_source_type == "player_metadata":
            player_metadata_dataset = handle_player_metadata_column(current_season_dataset_new)
            write_raw_to_bronze(player_metadata_dataset, password, container_name, adls_url_v2, data_source_type)
        else:
            check_data_types(current_season_dataset_new)
            write_raw_to_bronze(current_season_dataset_new, password, container_name, adls_url_v2, data_source_type)
    
        return func.HttpResponse(f"Data has been uploaded into bronze layer for data source - {data_source_type}", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)