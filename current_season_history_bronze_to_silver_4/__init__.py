from deltalake import DeltaTable, write_deltalake
import duckdb
from datetime import datetime
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
    dataset = pl.read_delta(azure_path, storage_options=credential)
    data_df = dataset.filter(pl.col("ingest_date") == ingest_date)
    logging.info(f"Created {data_source} dataset for ingest date = {ingest_date}")
    return data_df


def get_list_column(credential, data_source):
    adls_path = f"abfss://fantasy-premier-league@azfarsadev.dfs.core.windows.net/bronze/{data_source}"
    dataset_schema = pl.read_delta(source=adls_path, storage_options=credential).schema
    dataset_odict = dataset_schema.keys()
    dataset_list = list(dataset_odict)
    logging.info(f"Created dataset list")
    return dataset_list


def convert_column_datatype_current_season_history(df):
    dtype_mapping = {
        'element': pl.Int64,
        'fixture': pl.Int64,
        'opponent_team': pl.Int64,
        'total_points': pl.Int64,
        'was_home': pl.String,
        'kickoff_time': pl.Datetime("us"),
        'team_h_score': pl.Int64,
        'team_a_score': pl.Int64,
        'round': pl.Int64,
        'minutes': pl.Int64,
        'goals_scored': pl.Int64,
        'assists': pl.Int64,
        'clean_sheets': pl.Int64,
        'goals_conceded': pl.Int64,
        'own_goals': pl.Int64,
        'penalties_saved': pl.Int64,
        'penalties_missed': pl.Int64,
        'yellow_cards': pl.Int64,
        'red_cards': pl.Int64,
        'saves': pl.Int64,
        'bonus': pl.Int64,
        'bps': pl.Int64,
        'influence': pl.Float64,
        'creativity': pl.Float64,
        'threat': pl.Float64,
        'ict_index': pl.Float64,
        'starts': pl.Int64,
        'expected_goals': pl.Float64,
        'expected_assists': pl.Float64,
        'expected_goal_involvements': pl.Float64,
        'expected_goals_conceded': pl.Float64,
        'value': pl.Int64,
        'transfers_balance': pl.Int64,
        'selected': pl.Int64,
        'transfers_in': pl.Int64,
        'transfers_out': pl.Int64,
        'modified': pl.String,
        'ingest_date': pl.String
    }

    df = df.with_columns([
        pl.col(col_name).cast(dtype)
        for col_name, dtype in dtype_mapping.items()
    ])

    logging.info(f"Converted column datatype for current_season_history dataset")

    return df


def convert_column_datatype_player_metadata(df):
    dtype_mapping = {
        'chance_of_playing_next_round': pl.String,
        'chance_of_playing_this_round': pl.String,
        'code': pl.Int64,
        'cost_change_event': pl.Int64,
        'cost_change_event_fall': pl.Int64,
        'cost_change_start': pl.Int64,
        'cost_change_start_fall': pl.Int64,
        'dreamteam_count': pl.Int64,
        'element_type': pl.Int64,
        'ep_next': pl.Float64,
        'ep_this': pl.Float64,
        'event_points': pl.Int64,
        'first_name': pl.String,
        'form': pl.Float64,
        'id': pl.Int64,
        'in_dreamteam': pl.String,
        'news': pl.String,
        'news_added': pl.String,
        'now_cost': pl.Int64,
        'photo': pl.String,
        'points_per_game': pl.Float64,
        'second_name': pl.String,
        'selected_by_percent': pl.Float64,
        'special': pl.String,
        'squad_number': pl.String,
        'status': pl.String,
        'team': pl.Int64,
        'team_code': pl.Int64,
        'total_points': pl.Int64,
        'transfers_in': pl.Int64,
        'transfers_in_event': pl.Int64,
        'transfers_out': pl.Int64,
        'transfers_out_event': pl.Int64,
        'value_form': pl.Float64,
        'value_season': pl.Float64,
        'web_name': pl.String,
        'minutes': pl.Int64,
        'goals_scored': pl.Int64,
        'assists': pl.Int64,
        'clean_sheets': pl.Int64,
        'goals_conceded': pl.Int64,
        'own_goals': pl.Int64,
        'penalties_saved': pl.Int64,
        'penalties_missed': pl.Int64,
        'yellow_cards': pl.Int64,
        'red_cards': pl.Int64,
        'saves': pl.Int64,
        'bonus': pl.Int64,
        'bps': pl.Int64,
        'influence': pl.Float64,
        'creativity': pl.Float64,
        'threat': pl.Float64,
        'ict_index': pl.Float64,
        'starts': pl.Int64,
        'expected_goals': pl.Float64,
        'expected_assists': pl.Float64,
        'expected_goal_involvements': pl.Float64,
        'expected_goals_conceded': pl.Float64,
        'influence_rank': pl.Int64,
        'influence_rank_type': pl.Int64,
        'creativity_rank': pl.Int64,
        'creativity_rank_type': pl.Int64,
        'threat_rank': pl.Int64,
        'threat_rank_type': pl.Int64,
        'ict_index_rank': pl.Int64,
        'ict_index_rank_type': pl.Int64,
        'corners_and_indirect_freekicks_order': pl.String,
        'corners_and_indirect_freekicks_text': pl.String,
        'direct_freekicks_order': pl.String,
        'direct_freekicks_text': pl.String,
        'penalties_order': pl.String,
        'penalties_text': pl.String,
        'expected_goals_per_90': pl.Float64,
        'saves_per_90': pl.Float64,
        'expected_assists_per_90': pl.Float64,
        'expected_goal_involvements_per_90': pl.Float64,
        'expected_goals_conceded_per_90': pl.Float64,
        'goals_conceded_per_90': pl.Float64,
        'now_cost_rank': pl.Int64,
        'now_cost_rank_type': pl.Int64,
        'form_rank': pl.Int64,
        'form_rank_type': pl.Int64,
        'points_per_game_rank': pl.Int64,
        'points_per_game_rank_type': pl.Int64,
        'selected_rank': pl.Int64,
        'selected_rank_type': pl.Int64,
        'starts_per_90': pl.Float64,
        'clean_sheets_per_90': pl.Float64,
        'ingest_date': pl.Int64
    }

    df = df.with_columns([
        pl.col(col_name).cast(dtype)
        for col_name, dtype in dtype_mapping.items()
    ])

    logging.info(f"Converted column datatype for player_metadata dataset")

    return df


def convert_column_datatype_position_metadata(df):
    dtype_mapping = {
        'id': pl.String,
        'plural_name': pl.String,
        'plural_name_short': pl.String,
        'singular_name': pl.String,
        'singular_name_short': pl.String,
        'squad_select': pl.Int32,
        'squad_min_select': pl.String,
        'squad_max_select': pl.String,
        'squad_min_play': pl.Int32,
        'squad_max_play': pl.Int32,
        'ui_shirt_specific': pl.String,
        'element_count': pl.Int64,
        'ingest_date': pl.String
    }

    df = df.with_columns([
        pl.col(col_name).cast(dtype)
        for col_name, dtype in dtype_mapping.items()
    ])

    logging.info(f"Converted column datatype for position_metadata dataset")

    return df


def convert_column_datatype_team_metadata(df):
    dtype_mapping = {
        'code': pl.Int64,
        'form': pl.String,
        'id': pl.Int64,
        'name': pl.String,
        'draw': pl.Int64,
        'position': pl.Int64,
        'loss': pl.Int64,
        'strength_overall_home': pl.Int64,
        'unavailable': pl.String,
        'strength_overall_away': pl.Int64,
        'strength_attack_away': pl.Int64,
        'strength_defence_home': pl.Int64,
        'pulse_id': pl.Int64,
        'points': pl.Int64,
        'strength': pl.Int64,
        'team_division': pl.String,
        'played': pl.Int64,
        'win': pl.Int64,
        'strength_defence_away': pl.Int64,
        'strength_attack_home': pl.Int64,
        'short_name': pl.String,
        'ingest_date': pl.Int64
    }

    df = df.with_columns([
        pl.col(col_name).cast(dtype)
        for col_name, dtype in dtype_mapping.items()
    ])

    logging.info(f"Converted column datatype for team_metadata dataset")

    return df




def write_raw_to_bronze(dataset, storage_options, container_name, adls_url, layer, data_source):
    try:
        #write_deltalake(f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/{data_source}", dataset, storage_options=storage_options, mode='overwrite', engine='rust', schema_mode='overwrite', schema=column)
        dataset.write_delta(
            f"abfss://{container_name}@{adls_url}.dfs.core.windows.net/{layer}/{data_source}",
            mode="overwrite",
            delta_write_options={"schema_mode": "overwrite"},
            storage_options=storage_options
        )
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
    season = create_season_value(season_year)
    
    # Convert to Polars DataFrame if it isn't already
    if not isinstance(dataset, pl.DataFrame):
        dataset = pl.from_arrow(dataset)
    
    # Add new columns
    result = dataset.with_columns([
        pl.lit(season).alias('season'),
        pl.lit(datetime.now()).dt.strftime('%Y-%m-%d %H:%M:%S').alias('row_inserted_timestamp')
    ])
    
    logging.info(f"Add season column - {season} to dataset")
    
    return result


def convert_ingest_date_column_to_bigint(df_to_fix):
    df_to_fix = df_to_fix.with_columns(
        pl.col('ingest_date')
        .cast(pl.Int64)
        .alias("ingest_date")
    )

    logging.info(f"Ingest_date column in dataframe is converted to bigint")

    return df_to_fix



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

        if data_source_type == 'current_season_history':
            converted_column_dataset = convert_column_datatype_current_season_history(data_length)
        elif data_source_type == 'player_metadata':
            converted_column_dataset = convert_column_datatype_player_metadata(data_length)
        elif data_source_type == 'position_metadata':
            converted_column_dataset = convert_column_datatype_position_metadata(data_length)
        elif data_source_type == 'team_metadata':
            converted_column_dataset = convert_column_datatype_team_metadata(data_length)

        #dataset_column = get_list_column(password, data_source_type)
        #logging.info(f"Dataset column - {dataset_column}")
#
        #column_null_check = check_for_null(data_length)
        #if column_null_check != 0:
        #    logging.error("There is null")
        #else:
        #   logging.info(f"There is no null in the datasets")
        new_dataset = add_season_to_dataset(converted_column_dataset, file_date)
        #   logging.info(f"Season {create_season_value(file_date)} has been added to the dataset")
        #convert_dataset = convert_ingest_date_column_to_bigint(new_dataset)
        write_raw_to_bronze(new_dataset, password, container_name, adls_url, silver_layer, data_source_type)

        #print(str(convert_dataset.select('row_inserted_timestamp').head(5)), errors='replace')
        #   logging.info(f"{data_source_type} data for date {file_date} has been written to silver layer")

        return func.HttpResponse(f"Process Completed", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)