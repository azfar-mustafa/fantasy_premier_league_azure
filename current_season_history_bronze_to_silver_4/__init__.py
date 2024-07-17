from deltalake import DeltaTable
import duckdb
import pyarrow as pa
import azure.functions as func
import logging
from util.common_func import convert_timestamp_to_myt_date

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

def read_bronze_file():
    azure_path = "abfss://fantasy-premier-league@azfarsadev.dfs.core.windows.net/bronze/current_season_history"
    dt = DeltaTable(azure_path, storage_options=storage_options)
    dataset = dt.to_pyarrow_dataset()
    quack = duckdb.arrow(dataset)
    data = quack.filter("ingest_date = '17072024'")
    return data


def main(req: func.HttpRequest) -> func.HttpResponse:
    data_length = len(read_bronze_file())
    logging.info(f"Data length is {data_length}")
    return func.HttpResponse(f"Process Completed {data_length}", status_code=200)