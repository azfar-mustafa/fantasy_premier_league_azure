import logging
import tempfile
import azure.functions as func
import requests
import os
import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from util.common_func import convert_timestamp_to_myt_date


def download_blob(storage_account_url, container_name, local_temp_file_path, source_blob_path):
    try:
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(container_name)
        blob_client = container_client.get_blob_client(source_blob_path)

        # Reference - https://learn.microsoft.com/en-us/azure/storage/blobs/storage-blob-download-python
        with open(local_temp_file_path, "wb") as local_file:
            blob_data = blob_client.download_blob()
            local_file.write(blob_data.readall())

        logging.info(f"File {source_blob_path} has been downloaded locally")

        return True
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        return False
    

def read_local_file(local_temp_file_path):
    player_result = []
    try:
        with open(local_temp_file_path, "r") as json_file:
            main_json_file = json.load(json_file)

        for id_player in main_json_file:
                player_id = id_player.get("id")
                url = f"https://fantasy.premierleague.com/api/element-summary/{player_id}/"
                response = requests.get(url, timeout=60)
                if response.status_code == 200:
                    player_data = response.json()
                    current_season_past_fixture = player_data["history"]
                    player_result.extend(current_season_past_fixture)
                    logging.info(f"Player id - {player_id} is extracted")

        logging.info("Current season history player data has been extracted")
    
        return player_result
    except Exception as e:
        logging.error(f"Error in read_local_file: {str(e)}")
        raise


def create_file_and_upload(all_dict, player_id_local_file_path, storage_account_url, container_name, destination_blob_path):
    with open(player_id_local_file_path, "w") as local_file_player_id:
        for item in all_dict:
            json_line = json.dumps(item)
            local_file_player_id.write(json_line + '\n')
        logging.info(f"{player_id_local_file_path} is created")

    default_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_client = container_client.get_blob_client(destination_blob_path)
    with open(f"{player_id_local_file_path}", "rb") as data:
        blob_client.upload_blob(data, overwrite=True)

    logging.info(f"Current season history data has been uploaded to {destination_blob_path}")
    

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        ingest_date = req.params.get('ingest_date')

        if not ingest_date:
            return func.HttpResponse(
                "No parameter supplied. Please provide a 'ingest_date' based on the file to be ingested. Date format should be ddMMyyyy",
                status_code=400
            )
        
        storage_account_url = os.getenv("StorageAccountUrl")
        storage_account_container = os.getenv("StorageAccountContainer")
        blob_name = f"player_metadata_{ingest_date}.json"
        current_season_history_file_name = f"current_season_history_{ingest_date}.json"
        source_blob_path = f"landing/player_metadata/current/{ingest_date}/{blob_name}"
        destination_blob_path = f"landing/current_season_history/current/{ingest_date}/{current_season_history_file_name}"
        
        local_file_path = tempfile.gettempdir()
        local_temp_file_path = os.path.join(local_file_path, blob_name)
        player_id_local_file_path = os.path.join(local_file_path, current_season_history_file_name)
    
        download_blob(storage_account_url, storage_account_container, local_temp_file_path, source_blob_path)
        current_season_history_dict = read_local_file(local_temp_file_path)
        create_file_and_upload(current_season_history_dict, player_id_local_file_path, storage_account_url, storage_account_container, destination_blob_path)
    
        return func.HttpResponse(f"Process Completed", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)