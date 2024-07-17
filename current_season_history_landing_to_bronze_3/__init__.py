import os
import pandas as pd
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import BytesIO
import logging
from deltalake import write_deltalake
from azure.identity import DefaultAzureCredential

from util.common_func import convert_timestamp_to_myt_date, create_storage_options


def read_file_from_adls(directory_client, file_name):
    file_client = directory_client.get_file_client(file_name)
    download = file_client.download_file()
    downloaded_bytes = download.readall() # Output is in raw bytes
    logging.info("File is read from ADLS")
    return pd.read_json(BytesIO(downloaded_bytes))


def write_raw_to_bronze(dataset, storage_options, container_name, adls_url):
    try:
        write_deltalake(f"abfss://{container_name}@{adls_url}/bronze/current_season_history", dataset, storage_options=storage_options, mode='append', schema_mode='merge', engine='rust')
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
        container_name = os.getenv("StorageAccountContainer")
        adls_url = os.getenv("DataLakeUrl")
        current_date = convert_timestamp_to_myt_date()
        file_name = f"current_season_history_{current_date}.json"
        azure_dev_key_vault_url = os.getenv("KeyVault")
        credentials = DefaultAzureCredential()
        service_client = DataLakeServiceClient(account_url=adls_url, credential=credentials)
        password = create_storage_options(azure_dev_key_vault_url)
        directory_client = service_client.get_file_system_client(container_name).get_directory_client(f"landing/current_season_history/current/{current_date}")
        current_season_dataset = read_file_from_adls(directory_client, file_name)
        current_season_dataset_new = add_load_date_column(current_season_dataset, current_date)
        write_raw_to_bronze(current_season_dataset_new, password, container_name, adls_url)
    
        return func.HttpResponse(f"Process Completed {current_season_dataset_new.head()}", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)