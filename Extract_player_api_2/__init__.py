import logging
import tempfile
import azure.functions as func
import requests
import os
import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, ContainerClient, BlobPrefix
from util.common_func import convert_timestamp_to_myt_date
from typing import Tuple, Dict, Any, Optional, List, Union


def download_blob(storage_account_url: str, container_name: str, local_temp_file_path: str, source_blob_path: str) -> bool:
    """
    Downloads a blob from Azure Blob Storage to a specified local temporary file path.

    Args:
        storage_account_url (str): The URL of the Azure Storage account (e.g., "https://youraccount.blob.core.windows.net").
        container_name (str): The name of the container where the blob resides.
        local_temp_file_path (str): The full local path, including the filename, where the blob will be saved.
                                    (e.g., "C:/temp/downloaded_data.json").
        source_blob_path (str): The full path of the blob within the container (e.g., "landing/my_data.json").

    Returns:
        bool: True if the blob was successfully downloaded, False otherwise.
    """
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
    

def read_local_file(local_temp_file_path: str) -> Union[List[Dict[str, Any]], None]:
    """
    Reads a local JSONL file, extracts player IDs, fetches player summary data from an API,
    and returns current season historical fixture data for all players.

    Args:
        local_temp_file_path (str): The full path to the local JSONL file
                                    containing player ID information.

    Returns:
        Union[List[Dict[str, Any]], None]: A list of dictionaries, where each dictionary
                                            represents a player's past fixture data for the
                                            current season. Returns None if any error occurs
                                            during file reading or API calls.
    """
    player_result = []
    try:
        with open(local_temp_file_path, "r") as json_file:
            main_json_file = [json.loads(line) for line in json_file] 

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


def create_file_and_upload(all_dict: List[Dict[str, Any]], player_id_local_file_path: str, storage_account_url: str, container_name: str, destination_blob_path: str) -> bool:
    """
    Writes a list of dictionaries to a local JSON Lines (JSONL) file and then uploads
    that file to Azure Blob Storage.

    Args:
        all_dict (List[Dict[str, Any]]): A list of dictionaries to be written to the local file.
        player_id_local_file_path (str): The full local path, including the filename,
                                          where the JSONL file will be created.
                                          (e.g., "C:/temp/players.jsonl").
        storage_account_url (str): The URL of the Azure Storage account
                                   (e.g., "https://youraccount.blob.core.windows.net").
        container_name (str): The name of the target container in Blob Storage.
        destination_blob_path (str): The full path for the blob within the container
                                     (e.g., "processed_data/players.jsonl").

    Returns:
        bool: True if the file was successfully created locally and uploaded, False otherwise.
    """
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


def get_blob_name(storage_account_url: str, container_name: str) -> Optional[str]:
    """
    Connects to Azure Blob Storage, lists blobs starting with a specific prefix
    ('landing/raw_fpl_player_metadata_'), and returns the name of the *last* found blob
    after stripping the 'landing/' prefix.

    Args:
        storage_account_url (str): The URL of the Azure Storage account
                                   (e.g., "https://youraccount.blob.core.windows.net").
        container_name (str): The name of the container (file system) to search within.

    Returns:
        Optional[str]: The extracted name of the last matching blob (e.g., "raw_fpl_player_metadata_20250525.json"),
                       or None if no matching blob is found or an error occurs.
    """
    default_credential = DefaultAzureCredential()
    blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
    container_client = blob_service_client.get_container_client(container=container_name)

    blob_list = container_client.list_blob_names(name_starts_with=f'landing/raw_fpl_player_metadata_')

    for blob in blob_list:
        logging.info(f"File name: {blob}")
        blob_name = blob.split("landing/")[-1]

    return blob_name

    
    

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:
        ingest_date = req.params.get('ingest_date')

        if not ingest_date:
            return func.HttpResponse(
                "No parameter supplied. Please provide a 'ingest_date' based on the file to be ingested. Date format should be YYYYMMDD",
                status_code=400
            )
        
        current_timestamp = convert_timestamp_to_myt_date()
        
        storage_account_url = os.getenv("StorageAccountUrl")
        storage_account_container = os.getenv("StorageAccountContainer")
        player_metadata_file_name = get_blob_name(storage_account_url, storage_account_container)
        current_season_history_file_name = f"raw_fpl_current_season_history_{ingest_date}_{current_timestamp}.json"
        source_blob_path = f"landing/{player_metadata_file_name}"
        destination_blob_path = f"landing/{current_season_history_file_name}"
        
        local_file_path = tempfile.gettempdir()
        local_temp_file_path = os.path.join(local_file_path, player_metadata_file_name)
        player_id_local_file_path = os.path.join(local_file_path, current_season_history_file_name)
    
        download_blob(storage_account_url, storage_account_container, local_temp_file_path, source_blob_path)
        current_season_history_dict = read_local_file(local_temp_file_path)
        create_file_and_upload(current_season_history_dict, player_id_local_file_path, storage_account_url, storage_account_container, destination_blob_path)
    
        return func.HttpResponse(f"Process Completed", status_code=200)
    
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)