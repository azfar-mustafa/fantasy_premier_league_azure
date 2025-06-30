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
from datetime import datetime


def list_directory_contents(service_client: DataLakeServiceClient, container_name: str, directory_name: str, data_source: str, file_date: str) -> str:
    """
    Return a list containing file name based on the data source in landing folder

    Args:
        service_client (str): The credential to access ADLS2.
        container_name (str): The container storing the source files.
        directory_name (str): The path of source files in ADLS.
        data_source (str): Data source type to be processed.
        file_date (str): File date to be processed

    Returns:
        str: Source file name.
    """
    file_list = []
    file_system_client = service_client.get_file_system_client(container_name)
    paths = file_system_client.get_paths(path=directory_name)

    for path in paths:
        file_name = path.name.removeprefix("landing/")
        if data_source in file_name:
            file_list.append(file_name)
    
    logging.info(f"{file_list}")

    if not file_list:
        error_message = f"No files matching {data_source} in landing folder"
        logging.error(f"ERROR: {error_message}")
        raise Exception(error_message)
    
    if len(file_list) > 1:
        error_message = f"There are more than 1 file for {data_source} in landing folder"
        logging.error(f"ERROR: {error_message}")
        logging.info(f"File lists: {file_list}")
        raise Exception(error_message)
    
    if file_date in file_list[0]:
        logging.info(f"Source file for '{data_source}' exist for date '{file_date}'")
        return file_list[0]
    else:
        error_message = f"There is no file for '{data_source}' in landing folder with date {file_date}"
        logging.error(f"ERROR: {error_message}")
        raise Exception(error_message)


def read_file_from_adls_using_polars(credential: str, landing_file_name: str, adls_path: str) -> pl.DataFrame:
    """
    Return dataframe based on data type source stored in json

    Args:
        credential (str): The credential to access ADLS2.
        landing_file_name (str): The source file name in landing folder.
        adls_path (str): The adls2 path.
    Returns:
        pl.DataFrame: Dataset source read from landing folder.
    """
    landing_source_file_path = f"{adls_path}/landing/{landing_file_name}"
    df = pl.read_ndjson(landing_source_file_path, storage_options=credential)

    logging.info(f"Data from file {landing_file_name} can be read and stored in a dataframe")

    return df


def write_raw_to_landing(dataset: pl.DataFrame, storage_options: str, adls_path: str, data_source: str) -> None:
    """
    Insert dataframe into staging delta table

    Args:
        dataset (pl.DataFrame): Dataset from landing folder.
        storage_options (str): The credential to access ADLS2.
        adls_path (str): The adls2 path.
        data_source (str): Data source type to be processed.
    """
    try:
        dataset.write_delta(
            f"{adls_path}/staging/{data_source}",
            mode="overwrite",
            delta_write_options={
                "schema_mode": "overwrite"
                },
            storage_options=storage_options
        )
        logging.info(f"Dataset for {data_source} has been inserted into staging delta table")
    except Exception as e:
        error_msg = f"An error occured: {str(e)}"
        logging.error(error_msg)
        raise (error_msg)


def add_load_date_column(football_dataframe: pl.DataFrame, ingest_date: str) -> pl.DataFrame:
    """
    Return dataframe with addition column of ingest date and created timestamp

    Args:
        football_dataframe (pl.DataFrame): The dataset from landing folder.
        ingest_date (str): The ingest date based on file date
    Returns:
        pl.DataFrame: Dataset source read from landing folder.
    """
    created_timestamp = convert_timestamp_to_myt_date()
    df = football_dataframe.with_columns([
        pl.lit(ingest_date).alias('ingest_date'),
        pl.lit(created_timestamp).alias("created_timestamp")
    ])
    logging.info(f"Ingest date column with value {ingest_date} has been added")
    
    return df


def handle_inconsistent_null_value_columns(df: pl.DataFrame) -> pl.DataFrame:
    """
    Cast all columns to string data type and fill in null

    Args:
        df (pl.DataFrame): The dataset from landing folder.
    Returns:
        pl.DataFrame: Dataset source read from landing folder.
    """
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


def check_data_source(data_source: list, data_source_name: str) -> None:
    """
    Check input value of data source

    Args:
        data_source (list): Contain list of data source name.
        data_source_name (str): The input value of data source in API parameter.
    """
    if data_source_name in data_source:
        logging.info(f"Input value of data source - '{data_source_name}' is correct.")
    else:
        error_msg = f"Data source - '{data_source_name}' does not exists. Wrong value was input in the API parameter"
        logging.error(error_msg)
        raise ValueError(error_msg)


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
        data_source_list = ['current_season_history', 'player_metadata', 'team_metadata', 'position_metadata']
        check_data_source(data_source_list, data_source_type)

        default_credential = DefaultAzureCredential()
        StorageAccountName = os.getenv("StorageAccountName")
        container_name = os.getenv("StorageAccountContainer")
        adls_url_v2 = os.getenv("DataLakeUrllll")
        password = create_storage_options(os.getenv('KeyVault'))

        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net"

        service_client = DataLakeServiceClient(account_url=adls_url_v2, credential=default_credential)
        landing_file_name = list_directory_contents(service_client, container_name,'landing/', data_source_type, file_date)
        landing_df = read_file_from_adls_using_polars(password, landing_file_name, azure_path)
        current_season_dataset_new = add_load_date_column(landing_df, file_date)
        landing_data_to_load = handle_inconsistent_null_value_columns(current_season_dataset_new)
        write_raw_to_landing(landing_data_to_load, password, azure_path, data_source_type)
    
        return func.HttpResponse(f"Data has been uploaded into staging table for data source {data_source_type}", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)