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
    con = duckdb.connect()
    # Load PyArrow dataset into DuckDB
    #con.from_arrow('quack', dataset)
    # https://www.gooddata.com/blog/duckdb-meets-apache-arrow/
    query = f"SELECT * FROM dataset WHERE ingest_date = {ingest_date}"
    data_df = con.execute(query).arrow()
    return data_df


def check_for_null(dataset):
    con = duckdb.connect()
    con.register('data_table', dataset)
    #columns = con.execute("SELECT * FROM data_table LIMIT 0").df().columns.tolist()
    columns = ['element', 'fixture', 'opponent_team', 'kickoff_time', 'team_h_score', 'team_a_score', 'round', 'minutes', 'total_points', 'value', 'transfers_balance', 'selected', 'transfers_in','transfers_out','goals_scored', 'assists', 'clean_sheets']

    # Create a SQL query to count nulls for all columns
    null_counts_query = "+".join([f"SUM(CASE WHEN {col} IS NULL THEN 1 ELSE 0 END) " for col in columns])
    total_count_query = f"SELECT ({null_counts_query}) as 'total' FROM data_table"

    # Execute the query
    result = con.execute(f"{total_count_query}").df().iloc[0]
    total_nulls = int(result['total'])

    return total_nulls



def main(req: func.HttpRequest) -> func.HttpResponse:

    password = create_storage_options(os.getenv('KeyVault'))
    #current_date = convert_timestamp_to_myt_date()
    current_date = '17072024'
    data_length = read_bronze_file(current_date, password)

    column_name = check_for_null(data_length)
    if column_name != 0:
        logging.error("There is null")
    logging.info(f"There is no null in the datasets")
    return func.HttpResponse(f"Process Completed {column_name}", status_code=200)