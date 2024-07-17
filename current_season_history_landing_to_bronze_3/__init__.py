import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import BytesIO
import logging
from deltalake import DeltaTable, write_deltalake
from azure.identity import DefaultAzureCredential, ClientSecretCredential

from util.common_func import convert_timestamp_to_myt_date, create_storage_options


def read_file_from_adls(directory_client, file_name):
    file_client = directory_client.get_file_client(file_name)
    download = file_client.download_file()
    downloaded_bytes = download.readall() # Output is in raw bytes
    logging.info("File is read from ADLS")
    return pd.read_json(BytesIO(downloaded_bytes))


def write_raw_to_bronze(dataset, storage_options):
    try:
        write_deltalake("abfss://fantasy-premier-league@azfarsadev.dfs.core.windows.net/bronze/current_season_history", dataset, storage_options=storage_options, mode='append', schema_mode='merge', engine='rust')
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def add_load_date_column(football_dataframe, ingest_date):
    football_dataframe['ingest_date'] = ingest_date
    logging.info(f"Ingest date column with value {ingest_date} has been added")
    return football_dataframe



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        current_date = convert_timestamp_to_myt_date()
        file_name = f"current_season_history_{current_date}.json"
        azure_dev_key_vault_url = "https://azfar-keyvault-dev.vault.azure.net/"
        credentials = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url="https://azfarsadev.dfs.core.windows.net", credential=credentials)
        password = create_storage_options(azure_dev_key_vault_url)
        directory_client = service_client.get_file_system_client("fantasy-premier-league").get_directory_client(f"landing/current_season_history/current/{current_date}")
        current_season_dataset = read_file_from_adls(directory_client, file_name)
        current_season_dataset_new = add_load_date_column(current_season_dataset, current_date)
        write_raw_to_bronze(current_season_dataset_new, password)
    
        return func.HttpResponse(f"Process Completed {current_season_dataset_new.head()}", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)