from deltalake import DeltaTable, write_deltalake
import duckdb
import pyarrow as pa
from pyarrow import interchange
import pyarrow.compute as pc
import polars as pl
import azure.functions as func
import logging
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import os
from datetime import datetime


def read_bronze_file(ingest_date, credential, layer):
    container_name = os.getenv("StorageAccountContainer")
    StorageAccountName = os.getenv("StorageAccountName")
    azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/current_season_history"
    logging.info(f"Reading {azure_path}")
    dt = DeltaTable(azure_path, storage_options=credential)
    dataset = dt.to_pyarrow_dataset()
    con = duckdb.connect()
    # Load PyArrow dataset into DuckDB
    #con.from_arrow('quack', dataset)
    # https://www.gooddata.com/blog/duckdb-meets-apache-arrow/
    query = f"SELECT * FROM dataset WHERE ingest_date = {ingest_date}"
    data_df = con.execute(query).arrow()
    logging.info(f"Created current season history dataset for ingest date = {ingest_date}")
    return data_df


def check_for_null(dataset):
    con = duckdb.connect()
    con.register('data_table', dataset)
    #columns = con.execute("SELECT * FROM data_table LIMIT 0").df().columns.tolist()
    columns = ['element', 'fixture', 'opponent_team', 'kickoff_time', 'team_h_score', 'team_a_score', 'round', 'minutes', 'total_points', 'value', 'transfers_balance', 'selected', 'transfers_in','transfers_out','goals_scored', 'assists', 'clean_sheets']

    # Create a SQL query to count nulls for all columns
    null_counts_query = "OR".join([f" {col} IS NULL " for col in columns])
    has_null_query = f"""
        SELECT CASE WHEN EXISTS (
            SELECT 1 
            FROM data_table
            WHERE {null_counts_query}
        ) THEN 1 ELSE 0 END as has_null
        """

    # Execute the query
    result = con.execute(f"{has_null_query}").fetchone()
    total_nulls = int(result[0])

    return total_nulls



def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, layer):
    try:
        write_deltalake(f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/current_season_history", dataset, storage_options=storage_options, mode='append', schema_mode='merge', engine='rust')
        logging.info(f"Dataset has been inserted into {layer} layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def create_season_value(file_date):
    year = int(file_date[4:])
    month = int(file_date[2:4])

    month_a_list = [8,9,10,11,12]

    if month in month_a_list:
        year_a = year + 1
        actual_season = str(year) + '/' + str(year_a)
        return actual_season
    else:
        actual_year = year - 1
        actual_season = str(actual_year) + '/' + str(year)
        return actual_season



def add_season_to_dataset(dataset):
    
    pass


def add_unique_column(dataset):
    new_dataset = pl.from_arrow(dataset)
    return new_dataset


def main(req: func.HttpRequest) -> func.HttpResponse:
    try:
        adls_url = os.getenv("StorageAccountName")
        container_name = os.getenv("StorageAccountContainer")
        password = create_storage_options(os.getenv('KeyVault'))
        current_date = convert_timestamp_to_myt_date()
        bronze_layer = 'bronze'
        silver_layer = 'silver'
        data_length = read_bronze_file(current_date, password, bronze_layer)

        column_null_check = check_for_null(data_length)
        if column_null_check != 0:
            logging.error("There is null")
        else:
            logging.info(f"There is no null in the datasets")
            write_raw_to_bronze(data_length, password, container_name, adls_url, silver_layer)
            logging.info(f"Current season history data for date {current_date} has been written to silver layer")

        return func.HttpResponse(f"Process Completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)