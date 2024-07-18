from deltalake import DeltaTable
import duckdb
import pyarrow as pa
import azure.functions as func
import logging
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import os


def read_bronze_file(ingest_date, credential):
    container_name = os.getenv("StorageAccountContainer")
    StorageAccountName = os.getenv("StorageAccountName")
    azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/bronze/current_season_history"
    logging.info(f"{azure_path}")
    dt = DeltaTable(azure_path, storage_options=credential)
    dataset = dt.to_pyarrow_dataset()
    quack = duckdb.arrow(dataset)
    data = quack.filter(f"ingest_date = '{ingest_date}'")
    return data


def main(req: func.HttpRequest) -> func.HttpResponse:
    password = create_storage_options(os.getenv('KeyVault'))
    current_date = convert_timestamp_to_myt_date()
    data_length = len(read_bronze_file(current_date, password))
    logging.info(f"Data length is {data_length}")
    return func.HttpResponse(f"Process Completed {data_length}", status_code=200)