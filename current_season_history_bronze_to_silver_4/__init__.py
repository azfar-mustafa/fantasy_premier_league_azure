from deltalake import DeltaTable, write_deltalake
import duckdb
import polars as pl
import azure.functions as func
import logging
from util.common_func import create_storage_options
import os
import pandas as pd


def read_bronze_file(ingest_date, credential, layer, data_source):
    container_name = os.getenv("StorageAccountContainer")
    StorageAccountName = os.getenv("StorageAccountName")
    azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
    logging.info(f"Reading {azure_path}")
    dt = DeltaTable(azure_path, storage_options=credential)
    dataset = dt.to_pyarrow_dataset()
    con = duckdb.connect()
    con.register('dataset', dataset)
    # Load PyArrow dataset into DuckDB
    #con.from_arrow('quack', dataset)
    # https://www.gooddata.com/blog/duckdb-meets-apache-arrow/
    query = f"SELECT * FROM dataset WHERE ingest_date = {ingest_date}"
    data_df = con.execute(query).arrow()
    logging.info(f"Created {data_source} dataset for ingest date = {ingest_date}")
    return data_df


def get_list_column(credential, data_source):
    adls_path = f"abfss://fantasy-premier-league@azfarsadev.dfs.core.windows.net/bronze/{data_source}"
    dataset_schema = pl.read_delta(source=adls_path, storage_options=credential).schema
    dataset_odict = dataset_schema.keys()
    dataset_list = list(dataset_odict)
    logging.info(f"Created dataset list")
    return dataset_list


def check_for_null(dataset):
    pass
    #con = duckdb.connect()
    #con.register('data_table', dataset)
    ##columns = con.execute("SELECT * FROM data_table LIMIT 0").df().columns.tolist()
    #columns = ['element', 'fixture', 'opponent_team', 'kickoff_time', 'team_h_score', 'team_a_score', 'round', 'minutes', 'total_points', 'value', 'transfers_balance', 'selected', 'transfers_in','transfers_out','goals_scored', 'assists', 'clean_sheets']
#
    ## Create a SQL query to count nulls for all columns
    #null_counts_query = "OR".join([f" {col} IS NULL " for col in columns])
    #has_null_query = f"""
    #    SELECT CASE WHEN EXISTS (
    #        SELECT 1 
    #        FROM data_table
    #        WHERE {null_counts_query}
    #    ) THEN 1 ELSE 0 END as has_null
    #    """
#
    ## Execute the query
    #result = con.execute(f"{has_null_query}").fetchone()
    #total_nulls = int(result[0])
#
    #return total_nulls



def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, layer, data_source):
    try:
        write_deltalake(f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/{data_source}", dataset, storage_options=storage_options, mode='overwrite', engine='rust')
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



def add_season_to_dataset(dataset, season_year):
    con = duckdb.connect(':memory:')
    con.register('dataset', dataset)
    season = create_season_value(season_year)
    query = f"""
    SELECT *
    , '{season}' AS season
    , CURRENT_TIMESTAMP as row_inserted_timestamp
    FROM dataset
    """
    result = con.execute(query)

    logging.info(f"Add season column - {season} to dataset")

    return result.fetch_arrow_table()



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
        adls_url = os.getenv("StorageAccountName")
        container_name = os.getenv("StorageAccountContainer")
        password = create_storage_options(os.getenv('KeyVault'))
        bronze_layer = 'bronze'
        silver_layer = 'silver'
        logging.info(f"Data source - {data_source_type}")
        data_length = read_bronze_file(file_date, password, bronze_layer, data_source_type)

        dataset_column = get_list_column(password, data_source_type)
        logging.info(f"Dataset column - {dataset_column}")
#
        #column_null_check = check_for_null(data_length)
        #if column_null_check != 0:
        #    logging.error("There is null")
        #else:
        #   logging.info(f"There is no null in the datasets")
        new_dataset = add_season_to_dataset(data_length, file_date)
        #   logging.info(f"Season {create_season_value(file_date)} has been added to the dataset")
        write_raw_to_bronze(new_dataset, password, container_name, adls_url, silver_layer, data_source_type)
        #   logging.info(f"{data_source_type} data for date {file_date} has been written to silver layer")

        return func.HttpResponse(f"Process Completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)