from deltalake import DeltaTable
import duckdb
import pyarrow as pa
import azure.functions as func
import logging
from util.common_func import convert_timestamp_to_myt_date
import os

storage_options = {
        'AZURE_STORAGE_ACCOUNT_NAME': 'azfarsadev',
        #'AZURE_STORAGE_ACCOUNT_KEY': '', # Copy the key from access key section
        #'AZURE_STORAGE_ACCESS_KEY': '', # Copy the key from access key section
        #'AZURE_STORAGE_MASTER_KEY': '',
        #'AZURE_STORAGE_CLIENT_ID': '5410d93c-2a3e-4599-ac58-8e7b5b19f57c',
        #'AZURE_STORAGE_CLIENT_SECRET': '',
        #'AZURE_STORAGE_TENANT_ID': 'e45a8dcd-cacc-4155-b82e-c8d06c42d755',
        'AZURE_STORAGE_SAS_KEY': 'sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-07-17T10:07:55Z&st=2024-07-17T02:07:55Z&spr=https&sig=9n%2BPFPMBQcrwOdRVLsXRb9AJN8qOXsrv0cA7m6CEm1g%3D', # Generate SAS token and insert here
        #'AZURE_STORAGE_TOKEN': '',
        #'AZURE_STORAGE_USE_EMULATOR': '',
    }

def read_bronze_file(ingest_date):
    container_name = os.getenv("StorageAccountContainer")
    adls_url = os.getenv("DataLakeUrl")
    azure_path = f"abfss://{container_name}@{adls_url}/bronze/current_season_history"
    dt = DeltaTable(azure_path, storage_options=storage_options)
    dataset = dt.to_pyarrow_dataset()
    quack = duckdb.arrow(dataset)
    data = quack.filter(f"ingest_date = '{ingest_date}'")
    return data


def main(req: func.HttpRequest) -> func.HttpResponse:
    current_date = convert_timestamp_to_myt_date()
    data_length = len(read_bronze_file(current_date))
    logging.info(f"Data length is {data_length}")
    return func.HttpResponse(f"Process Completed {data_length}", status_code=200)