import os
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from io import BytesIO
import logging
from deltalake import write_deltalake
from azure.identity import DefaultAzureCredential
import pyarrow as pa
from util.common_func import convert_timestamp_to_myt_date, create_storage_options
import polars as pl


def create_season_value(file_date):
    year = int(file_date[4:])
    month = int(file_date[2:4])

    month_a_list = [8,9,10,11,12]

    if month in month_a_list:
        year_a = int(year) + 1
        actual_season = str(year) + '/' + str(year_a)
        logging.info(f"Actual season: {actual_season}")
        return actual_season
    else:
        actual_year = int(year) - 1
        actual_season = str(actual_year) + '/' + str(year)
        logging.info(f"Actual season: {actual_season}")
        return actual_season


def list_of_columns(data_source):
    if data_source == 'player_metadata':
        column_list = ['influence_rank_type' ,'selected_rank_type' ,'transfers_out_event' ,'mng_underdog_win' ,'corners_and_indirect_freekicks_text' ,'minutes' ,'has_temporary_code' ,'opta_code' ,'expected_assists_per_90' ,'team' ,'assists' ,'clean_sheets_per_90' ,'now_cost' ,'mng_clean_sheets' ,'second_name' ,'selected_by_percent' ,'birth_date' ,'special' ,'mng_win' ,'region' ,'creativity_rank' ,'transfers_in_event' ,'clean_sheets' ,'value_form' ,'penalties_saved' ,'saves_per_90' ,'selected_rank' ,'status' ,'photo' ,'can_transact' ,'expected_goal_involvements' ,'transfers_out' ,'cost_change_event_fall' ,'starts_per_90' ,'starts' ,'total_points' ,'mng_underdog_draw' ,'ep_next' ,'red_cards' ,'code' ,'cost_change_start_fall' ,'first_name' ,'mng_goals_scored' ,'direct_freekicks_order' ,'penalties_text' ,'direct_freekicks_text' ,'expected_goals_per_90' ,'expected_goals_conceded_per_90' ,'value_season' ,'form_rank_type' ,'points_per_game_rank' ,'chance_of_playing_next_round' ,'can_select' ,'goals_scored' ,'ict_index' ,'corners_and_indirect_freekicks_order' ,'now_cost_rank_type' ,'points_per_game_rank_type' ,'form' ,'dreamteam_count' ,'news_added' ,'bps' ,'threat' ,'influence' ,'threat_rank' ,'in_dreamteam' ,'influence_rank' ,'threat_rank_type' ,'own_goals' ,'ep_this' ,'now_cost_rank' ,'team_join_date' ,'id' ,'chance_of_playing_this_round' ,'news' ,'penalties_missed' ,'element_type' ,'expected_assists' ,'ict_index_rank' ,'transfers_in' ,'ict_index_rank_type' ,'mng_draw' ,'penalties_order' ,'removed' ,'cost_change_start' ,'web_name' ,'bonus' ,'goals_conceded_per_90' ,'event_points' ,'form_rank' ,'goals_conceded' ,'creativity_rank_type' ,'points_per_game' ,'squad_number' ,'yellow_cards' ,'creativity' ,'expected_goals_conceded' ,'mng_loss' ,'cost_change_event' ,'team_code' ,'expected_goals' ,'saves' ,'expected_goal_involvements_per_90', 'ingest_date']
    elif data_source == 'current_season_history':
        column_list = ['expected_goal_involvements', 'transfers_in', 'mng_clean_sheets', 'goals_scored', 'expected_assists', 'element', 'fixture', 'minutes', 'assists', 'own_goals', 'ict_index', 'expected_goals', 'transfers_out', 'was_home', 'total_points', 'selected', 'value', 'mng_win', 'team_a_score', 'modified', 'threat', 'penalties_saved', 'yellow_cards', 'round', 'red_cards', 'opponent_team', 'bps', 'expected_goals_conceded', 'penalties_missed', 'bonus', 'saves', 'mng_draw', 'mng_loss', 'mng_underdog_draw', 'clean_sheets', 'kickoff_time', 'creativity', 'goals_conceded', 'starts', 'team_h_score', 'mng_underdog_win', 'influence', 'mng_goals_scored', 'transfers_balance', 'ingest_date']
    elif data_source == 'team_metadata':
        column_list = ['code', 'draw', 'form', 'id', 'loss', 'name', 'played', 'points', 'position', 'short_name', 'strength', 'team_division', 'unavailable', 'win', 'strength_overall_home', 'strength_overall_away', 'strength_attack_home', 'strength_attack_away', 'strength_defence_home', 'strength_defence_away', 'pulse_id', 'ingest_date']
    elif data_source == 'position_metadata':
        column_list = ['id', 'plural_name', 'plural_name_short', 'singular_name', 'singular_name_short', 'squad_select', 'squad_min_select', 'squad_max_select', 'squad_min_play', 'squad_max_play', 'ui_shirt_specific', 'element_count', 'ingest_date']

    
    return column_list



def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, data_source):
    try:
        dataset.write_delta(
            f"abfss://{container_name}@{adls_url}/bronze/{data_source}",
            mode="append",
            storage_options=storage_options
        )
        logging.info("Dataset has been inserted into bronze layer")
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")


def add_load_date_column(football_dataframe, season):
    created_timestamp = convert_timestamp_to_myt_date()
    df = football_dataframe.with_columns([
        pl.lit(season).alias("season"),
        pl.lit(created_timestamp).alias("created_timestamp")
    ])
    logging.info(f"Added value in dataframe for column season - {season} and created_timestamp - {created_timestamp}")
    
    return df


def detect_new_or_changed_rows(staging_df, bronze_df):
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

    logging.info(f"Comparing row difference between staging and bronze table")

    return df_diff


def get_delta_table_column_list(credential, layer, data_source):
    try:
        container_name = os.getenv("StorageAccountContainer")
        StorageAccountName = os.getenv("StorageAccountName")
        azure_path = f"abfss://{container_name}@{StorageAccountName}.dfs.core.windows.net/{layer}/{data_source}"
        dataset = pl.read_delta(azure_path, storage_options=credential)
        delta_table_column_list = dataset.columns
        logging.info(f"Column lists have been extracted from layer {layer} for {data_source}")
        return delta_table_column_list
    except Exception as e:
        logging.error(f"Removed column fails: {str(e)}")
    


def compare_columns(delta_table_column_list, staging_column_list):
    ignore_columns = {'season', 'created_timestamp'}

    landing_set = set(delta_table_column_list) - ignore_columns
    json_set = set(staging_column_list) - ignore_columns
    if sorted(landing_set) == sorted(json_set):
        logging.info("All columns match between staging and delta table")
    else:
        missing_in_json = landing_set - json_set
        missing_in_landing = json_set - landing_set
        if missing_in_json:
            logging.error(f"Columns missing in JSON: {missing_in_json}")
        if missing_in_landing:
            logging.error(f"Columns missing in landing: {missing_in_landing}")


def read_delta_table(credential, layer, data_source, column_list):
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
        column_list.append('season')
        logging.info(f"Season column is added in dataframe")
        selected_dataset = dataset.select(column_list)

    return selected_dataset


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
        staging_column_list = list_of_columns(data_source_type)
        bronze_column_list = get_delta_table_column_list(password, 'bronze', data_source_type)
        compare_columns(bronze_column_list, staging_column_list)
        staging_df = read_delta_table(password, 'staging', data_source_type, staging_column_list)
        current_season_dataset_new = add_load_date_column(staging_df, season)
        bronze_df = read_delta_table(password, 'bronze', data_source_type, staging_column_list)
        new_data = detect_new_or_changed_rows(current_season_dataset_new, bronze_df)

        if new_data.is_empty() == False:
            logging.info("There is new data to be append")
            write_raw_to_bronze(new_data, password, container_name, adls_url_v2, data_source_type)
            return func.HttpResponse(f"Data has been uploaded into bronze layer for data source {data_source_type}", status_code=200)
        else:
            logging.info("No new data to be append")
            return func.HttpResponse(f"No data has need to be uploaded into bronze layer for data source {data_source_type}", status_code=200)
        
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)