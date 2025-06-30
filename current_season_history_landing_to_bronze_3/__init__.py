import os
import azure.functions as func
import logging
from datetime import datetime
from azure.identity import DefaultAzureCredential
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import polars as pl
from deltalake import DeltaTable

"""
This code is to move data from staging delta table to bronze delta table.
It also create custom value such as season, composite key to join 4 base tables.
"""


def create_season_value(file_date: str) -> str:
    """
    Create season value.

    Args:
        file_date (str): The value of date to be used as indicator to create season value.

    Returns:
        str: Season value. Example value is 2024/2025.
    """
    year = int(file_date[4:])
    month = int(file_date[2:4])

    month_list = [8,9,10,11,12]

    if len(file_date) != 8:
        error_msg = f"File date value - {file_date} is wrong"
        logging.error(error_msg)
        raise ValueError(error_msg)

    try:
        date_object = datetime.strptime(file_date, '%d%m%Y')
        year = date_object.year
        month = date_object.month


        if month in month_list:
            year_a = int(year) + 1
            actual_season = str(year) + '/' + str(year_a)
            logging.info(f"Actual season: {actual_season}")
            return actual_season
        else:
            actual_year = int(year) - 1
            actual_season = str(actual_year) + '/' + str(year)
            logging.info(f"Actual season: {actual_season}")
            return actual_season
    except ValueError as e:
        error_msg = f"Invalid file_date format or value: '{file_date}'. Expected 'ddMMyyyy'. Error: {e}"
        logging.error(error_msg)
        raise ValueError(error_msg)


def write_raw_to_bronze(dataset: pl.DataFrame, storage_options: dict, container_name: str, adls_url: str, data_source: str) -> None:
    """
    Write data to delta table in bronze layer.

    Args:
        dataset (pl.dataframe): Dataset to be written to delta table.
        storage_options (dict): Credentials to access ADLS2.
        container_name (str): Container of delta table.
        adls_url (str): The URL of the Azure Storage account (e.g., "https://youraccount.blob.core.windows.net").
        data_source (str): The value of data source to be processed.
    """
    try:
        dataset.write_delta(
            f"abfss://{container_name}@{adls_url}/bronze/{data_source}",
            mode="append",
            storage_options=storage_options
        )
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        raise


def add_season_column(football_dataframe: pl.DataFrame, season: str) -> pl.DataFrame:
    """
    Add season column to the staging dataframe.

    Args:
        football_dataframe (pl.DataFrame): The dataset to be append.
        season (str): The season value to be added as a column.

    Returns:
        pl.DataFrame: Return dataframe.
    """
    df = football_dataframe.with_columns([
        pl.lit(season).alias("season")
    ])
    logging.info(f"Added value in dataframe for column season - {season}")
    
    return df


def add_load_date_column(football_dataframe: pl.DataFrame) -> pl.DataFrame:
    """
    Add timestamp column to the staging dataframe.

    Args:
        football_dataframe (pl.DataFrame): The dataset to be append.

    Returns:
        pl.DataFrame: Return dataframe with appended timestamp column.
    """
    created_timestamp = convert_timestamp_to_myt_date()
    df = football_dataframe.with_columns([
        pl.lit(created_timestamp).alias("created_timestamp")
    ])
    logging.info(f"Added value in dataframe for column created_timestamp - {created_timestamp}")
    
    return df


def detect_new_or_changed_rows(staging_df: pl.DataFrame, bronze_df: pl.DataFrame, data_source: str) -> pl.DataFrame:
    """
    Compare new or changed rows between staging against bronze dataframe.

    Args:
        staging_df (pl.DataFrame): The staging dataframe from staging delta table.
        bronze_df (pl.DataFrame): The bronze dataframe from bronze delta table.
        data_source (str): The value of data source to be processed.

    Returns:
        pl.DataFrame: Return dataframe if there is new or updated rows from staging table.
    """
    try:
        exclude_columns = ['ingest_date', 'created_timestamp']

        cols_to_hash = [col for col in staging_df.columns if col not in exclude_columns]

        df_staging_hashed = staging_df.with_columns([
            pl.struct(cols_to_hash).hash().alias("row_hash")
        ])

        df_bronze_hashed = bronze_df.with_columns([
            pl.struct(cols_to_hash).hash().alias("row_hash")
        ])

        df_diff = df_staging_hashed.join(
            df_bronze_hashed.select(["row_hash"]),
            on="row_hash",
            how="anti"
        ).drop("row_hash")

        logging.info(f"Comparing row difference between staging and bronze table for {data_source}")

    except Exception as e:
        logging.error(f"Column {e} is missing from bronze table. Please add the column in bronze table for {data_source}")

    return df_diff


def get_delta_table_column_list(credential: str, layer: str, data_source: str, azure_path: str) -> list:
    """
    Get lists of column from bronze or silver delta table.

    Args:
        credential (str): The credential to access ADLS2.
        layer (str): The layer of delta table to be accessed.
        data_source (str): The value of data source to be processed.
        azure_path (str): The value of ADLS2 url.

    Returns:
        list: Return a list containing column names.
    """
    try:
        delta_table_path = f"{azure_path}/{layer}/{data_source}"
        dataset = pl.scan_delta(delta_table_path, storage_options=credential)
        delta_table_column_list = dataset.collect_schema().names()
        logging.info(f"Column lists have been extracted from layer {layer} for {data_source}")
        return delta_table_column_list
    except Exception as e:
        logging.error(f"Removed column fails: {str(e)}")
        raise
    


def compare_columns(delta_table_column_list: list, staging_column_list: list, data_source: str) -> None:
    """
    To compare if there's new column in staging table

    Args:
        delta_table_column_list (list): The list of column from delta table.
        staging_column_list (list): The list of column from staging table.
        data_source (str): The value of data source to be processed.
    """
    # Add column name in dictionary below for custom added column in bronze layer table
    ignore_columns = {'season', 'created_timestamp', 'player_current_season_history_key', 'player_team_key', 'player_position_key'}

    landing_set = set(delta_table_column_list) - ignore_columns
    json_set = set(staging_column_list) - ignore_columns
    if sorted(landing_set) == sorted(json_set):
        logging.info(f"All columns match between staging and bronze delta table for {data_source}")
    else:
        missing_in_json = landing_set - json_set
        missing_in_landing = json_set - landing_set
        if missing_in_json:
            logging.error(f"Columns missing in JSON: {missing_in_json} for {data_source}")
        if missing_in_landing:
            logging.error(f"Columns missing in landing: {missing_in_landing} for {data_source}")


def read_delta_table(credential: str, layer: str, data_source: str, column_list: list, azure_path: str) -> pl.DataFrame:
    """
    Return dataset based on selected columns

    Args:
        credential (str): The credential to access ADLS2.
        layer (str): The layer of delta table to be accessed.
        data_source (str): The value of data source to be processed.
        column_list (list): The list of column names for column to be selected in dataset.
        azure_path (str): The path of ADLS2 url

    Returns:
        pl.DataFrame: Return the selected dataframe.
    """
    delta_table_path = f"{azure_path}/{layer}/{data_source}"
    logging.info(f"Reading {delta_table_path}")
    dataset = pl.read_delta(delta_table_path, storage_options=credential)
    logging.info(f"Reading data from delta table for {data_source} in {layer} layer")
    selected_dataset = dataset.select(column_list)

    return selected_dataset


def create_composite_key(football_dataframe: pl.DataFrame, data_source: str) -> pl.DataFrame:
    """
    Add composite key in bronze dataframe

    Args:
        football_dataframe (pl.DataFrame): The dataframe of bronze delta table.
        data_source (str): The layer of delta table to be accessed.

    Returns:
        pl.DataFrame: Return the dataframe with additional columns of composite key.
    """
    if data_source == 'player_metadata':
        composite_key_column = ["player_current_season_history_key", "player_team_key", "player_position_key"]
        df = football_dataframe.with_columns([
            pl.concat_str(
                [
                    pl.col("id"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column[0]}"),
            pl.concat_str(
                [
                    pl.col("team"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column[1]}"),
            pl.concat_str(
                [
                    pl.col("element_type"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column[2]}")
        ]
        )
        logging.info(f"Added composite key column {composite_key_column[0]}, {composite_key_column[1]}, {composite_key_column[2]} in {data_source} for bronze layer table")
    elif data_source == 'current_season_history':
        composite_key_column = "player_current_season_history_key"
        df = football_dataframe.with_columns(
            pl.concat_str(
                [
                    pl.col("element"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column}"),
        )
        logging.info(f"Added composite key column - {composite_key_column} in {data_source} for bronze layer table")
    elif data_source == 'team_metadata':
        composite_key_column = "player_team_key"
        df = football_dataframe.with_columns(
            pl.concat_str(
                [
                    pl.col("id"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column}"),
        )
        logging.info(f"Added composite key column - {composite_key_column} in {data_source} for bronze layer table")
    elif data_source == 'position_metadata':
        composite_key_column = "player_position_key"
        df = football_dataframe.with_columns(
            pl.concat_str(
                [
                    pl.col("id"),
                    pl.col("season")
                ],
                separator="-",
            ).alias(f"{composite_key_column}"),
        )
        logging.info(f"Added composite key column - {composite_key_column} in {data_source} for bronze layer table")

    return df


def remove_row(azure_path: str, date_to_delete: str, data_source: str, storage_options: str):
    """
    Delete rows based on date

    Args:
        dataset (pl.DataFrame): The dataframe of bronze delta table.
        date_to_delete (str): The date that is used to delete rows.

    Returns:
        pl.DataFrame: Return the dataframe with deleted rows based on date.
    """
    table_path = f"{azure_path}/bronze/{data_source}"
    dt = DeltaTable(table_path, storage_options=storage_options)
    predicate = f"ingest_date = '{date_to_delete}'"
    deleted_rows_info = dt.delete(predicate=predicate)
    num_deleted_rows = deleted_rows_info['num_deleted_rows']
    logging.info(f"Removing {num_deleted_rows} rows from bronze polars dataframe for date {date_to_delete}")


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
        # Get ADLS credential details
        container_name = os.getenv("StorageAccountContainer")
        adls_url_v2 = os.getenv("DataLakeUrllll")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net"
        password = create_storage_options(os.getenv('KeyVault'))

        data_source_list = ['current_season_history', 'player_metadata', 'team_metadata', 'position_metadata']
        check_data_source(data_source_list, data_source_type)

        season = create_season_value(file_date)
        staging_column_list = get_delta_table_column_list(password, 'staging', data_source_type, azure_path)
        bronze_column_list = get_delta_table_column_list(password, 'bronze', data_source_type, azure_path)
        compare_columns(bronze_column_list, staging_column_list, data_source_type)
        staging_df = read_delta_table(password, 'staging', data_source_type, staging_column_list, azure_path)
        current_season_dataset_season_new = add_season_column(staging_df, season)
        add_composite_key = create_composite_key(current_season_dataset_season_new, data_source_type)
        current_season_dataset_new = add_load_date_column(add_composite_key)
        bronze_df = read_delta_table(password, 'bronze', data_source_type, bronze_column_list, azure_path)
        new_data = detect_new_or_changed_rows(current_season_dataset_new, bronze_df, data_source_type)

        if new_data.is_empty() == False:
            logging.info("There is new data to be append")
            write_raw_to_bronze(new_data, password, container_name, adls_url_v2, data_source_type)
            return func.HttpResponse(f"Data has been uploaded into bronze layer for data source {data_source_type}", status_code=200)
        else:
            logging.info("No new data to be append")
            return func.HttpResponse(f"No data has need to be uploaded into bronze layer for data source {data_source_type}", status_code=200)
        
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)