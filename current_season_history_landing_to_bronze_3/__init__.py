import os
import azure.functions as func
import logging
from azure.identity import DefaultAzureCredential
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import polars as pl

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


def list_of_staging_column(data_source: str) -> list:
    """
    Provide list of column in staging delta table.

    Args:
        data_source (str): The value of data source to be processed.

    Returns:
        list: Return column list.
    """
    if data_source == 'player_metadata':
        column_list = ['influence_rank_type' ,'selected_rank_type' ,'transfers_out_event' ,'mng_underdog_win' ,'corners_and_indirect_freekicks_text' ,'minutes' ,'has_temporary_code' ,'opta_code' ,'expected_assists_per_90' ,'team' ,'assists' ,'clean_sheets_per_90' ,'now_cost' ,'mng_clean_sheets' ,'second_name' ,'selected_by_percent' ,'birth_date' ,'special' ,'mng_win' ,'region' ,'creativity_rank' ,'transfers_in_event' ,'clean_sheets' ,'value_form' ,'penalties_saved' ,'saves_per_90' ,'selected_rank' ,'status' ,'photo' ,'can_transact' ,'expected_goal_involvements' ,'transfers_out' ,'cost_change_event_fall' ,'starts_per_90' ,'starts' ,'total_points' ,'mng_underdog_draw' ,'ep_next' ,'red_cards' ,'code' ,'cost_change_start_fall' ,'first_name' ,'mng_goals_scored' ,'direct_freekicks_order' ,'penalties_text' ,'direct_freekicks_text' ,'expected_goals_per_90' ,'expected_goals_conceded_per_90' ,'value_season' ,'form_rank_type' ,'points_per_game_rank' ,'chance_of_playing_next_round' ,'can_select' ,'goals_scored' ,'ict_index' ,'corners_and_indirect_freekicks_order' ,'now_cost_rank_type' ,'points_per_game_rank_type' ,'form' ,'dreamteam_count' ,'news_added' ,'bps' ,'threat' ,'influence' ,'threat_rank' ,'in_dreamteam' ,'influence_rank' ,'threat_rank_type' ,'own_goals' ,'ep_this' ,'now_cost_rank' ,'team_join_date' ,'id' ,'chance_of_playing_this_round' ,'news' ,'penalties_missed' ,'element_type' ,'expected_assists' ,'ict_index_rank' ,'transfers_in' ,'ict_index_rank_type' ,'mng_draw' ,'penalties_order' ,'removed' ,'cost_change_start' ,'web_name' ,'bonus' ,'goals_conceded_per_90' ,'event_points' ,'form_rank' ,'goals_conceded' ,'creativity_rank_type' ,'points_per_game' ,'squad_number' ,'yellow_cards' ,'creativity' ,'expected_goals_conceded' ,'mng_loss' ,'cost_change_event' ,'team_code' ,'expected_goals' ,'saves' ,'expected_goal_involvements_per_90', 'ingest_date']
    elif data_source == 'current_season_history':
        column_list = ['expected_goal_involvements', 'transfers_in', 'mng_clean_sheets', 'goals_scored', 'expected_assists', 'element', 'fixture', 'minutes', 'assists', 'own_goals', 'ict_index', 'expected_goals', 'transfers_out', 'was_home', 'total_points', 'selected', 'value', 'mng_win', 'team_a_score', 'modified', 'threat', 'penalties_saved', 'yellow_cards', 'round', 'red_cards', 'opponent_team', 'bps', 'expected_goals_conceded', 'penalties_missed', 'bonus', 'saves', 'mng_draw', 'mng_loss', 'mng_underdog_draw', 'clean_sheets', 'kickoff_time', 'creativity', 'goals_conceded', 'starts', 'team_h_score', 'mng_underdog_win', 'influence', 'mng_goals_scored', 'transfers_balance', 'ingest_date']
    elif data_source == 'team_metadata':
        column_list = ['code', 'draw', 'form', 'id', 'loss', 'name', 'played', 'points', 'position', 'short_name', 'strength', 'team_division', 'unavailable', 'win', 'strength_overall_home', 'strength_overall_away', 'strength_attack_home', 'strength_attack_away', 'strength_defence_home', 'strength_defence_away', 'pulse_id', 'ingest_date']
    elif data_source == 'position_metadata':
        column_list = ['id', 'plural_name', 'plural_name_short', 'singular_name', 'singular_name_short', 'squad_select', 'squad_min_select', 'squad_max_select', 'squad_min_play', 'squad_max_play', 'ui_shirt_specific', 'element_count', 'ingest_date']

    return column_list



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
            mode="overwrite",
            storage_options=storage_options
        )
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


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


def get_delta_table_column_list(credential: str, layer: str, data_source: str) -> list:
    """
    Get lists of column from bronze or silver delta table.

    Args:
        credential (str): The credential to access ADLS2.
        layer (str): The layer of delta table to be accessed.
        data_source (str): The value of data source to be processed.

    Returns:
        list: Return a list containing column names.
    """
    try:
        container_name = os.getenv("StorageAccountContainer")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
        dataset = pl.scan_delta(azure_path, storage_options=credential)
        delta_table_column_list = dataset.collect_schema().names()
        logging.info(f"Column lists have been extracted from layer {layer} for {data_source}")
        return delta_table_column_list
    except Exception as e:
        logging.error(f"Removed column fails: {str(e)}")
    


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


def read_delta_table(credential: str, layer: str, data_source: str, column_list: list) -> pl.DataFrame:
    """
    Return dataset based on selected columns

    Args:
        credential (str): The credential to access ADLS2.
        layer (str): The layer of delta table to be accessed.
        data_source (str): The value of data source to be processed.
        column_list (list): The list of column names for column to be selected in dataset.

    Returns:
        pl.DataFrame: Return the selected dataframe.
    """
    if layer == 'staging':
        container_name = os.getenv("StorageAccountContainer")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
        logging.info(f"Reading {azure_path}")
        dataset = pl.read_delta(azure_path, storage_options=credential)
        selected_dataset = dataset.select(column_list)
    else:
        container_name = os.getenv("StorageAccountContainer")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
        logging.info(f"Reading {azure_path}")
        dataset = pl.read_delta(azure_path, storage_options=credential)
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


def remove_row(dataset: pl.DataFrame, date_to_delete: str):
    """
    Delete rows based on date

    Args:
        dataset (pl.DataFrame): The dataframe of bronze delta table.
        date_to_delete (str): The date that is used to delete rows.

    Returns:
        pl.DataFrame: Return the dataframe with deleted rows based on date.
    """
    logging.info(f"Removing rows from bronze polars dataframe for date {date_to_delete}")
    return dataset.remove(pl.col("ingest_date") == date_to_delete)



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
        default_credential = DefaultAzureCredential()
        container_name = os.getenv("StorageAccountContainer")
        adls_url_v2 = os.getenv("DataLakeUrllll")
        password = create_storage_options(os.getenv('KeyVault'))
        season = create_season_value(file_date)
        staging_column_list = list_of_staging_column(data_source_type)
        bronze_column_list = get_delta_table_column_list(password, 'bronze', data_source_type)
        compare_columns(bronze_column_list, staging_column_list, data_source_type)
        staging_df = read_delta_table(password, 'staging', data_source_type, staging_column_list)
        current_season_dataset_season_new = add_season_column(staging_df, season)
        add_composite_key = create_composite_key(current_season_dataset_season_new, data_source_type)
        current_season_dataset_new = add_load_date_column(add_composite_key)
        bronze_df = read_delta_table(password, 'bronze', data_source_type, bronze_column_list)
        bronze_df_new = remove_row(bronze_df, file_date)
        new_data = detect_new_or_changed_rows(current_season_dataset_new, bronze_df_new, data_source_type)

        if new_data.is_empty() == False:
            logging.info("There is new data to be append")
            write_raw_to_bronze(new_data, password, container_name, adls_url_v2, data_source_type)
            return func.HttpResponse(f"Data has been uploaded into bronze layer for data source {data_source_type}", status_code=200)
        else:
            logging.info("No new data to be append")
            return func.HttpResponse(f"No data has need to be uploaded into bronze layer for data source {data_source_type}", status_code=200)
        
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)