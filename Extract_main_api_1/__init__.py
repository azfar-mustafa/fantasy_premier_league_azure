import logging
import os
import tempfile
import azure.functions as func
import requests
import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from util.common_func import convert_timestamp_to_myt_date
from typing import Tuple, Dict, Any, Optional, List, Union


def fetch_data_api(website_url: str) -> Optional[Tuple[List[Dict[str, Any]]]]:
    """
    Fetches JSON data from a given API URL, processes it, and returns specific parts.

    Args:
        website_url (str): The URL of the API endpoint to fetch data from.

    Returns:
        Optional[Tuple[List[Dict[str, Any]], ...]]: A tuple containing lists of dictionaries
        for 'events', 'teams', 'elements', and 'element_types' if successful.
        Returns None if an error occurs during the API call or data processing.
    """
    try:
        player_team_detail_url = website_url
        response = requests.get(player_team_detail_url, stream=True, timeout=2)
        response.raise_for_status()
        data = response.json()
        processed_data = remove_key_in_position(data)
        return processed_data["events"], processed_data["teams"], processed_data["elements"], processed_data["element_types"]
    except Exception as e:
        logging.error(f"An error occured: {e}")
        return None
    

def remove_key_in_position(position_data: Dict[str, Any]) -> Dict[str, Any]:
    """
    Removes the "sub_positions_locked" key from each dictionary
    within the "element_types" list in the input data.

    Args:
        position_data (Dict[str, Any]): A dictionary expected to contain an
                                        "element_types" key, whose value is
                                        a list of dictionaries.

    Returns:
        Dict[str, Any]: The modified dictionary after removing the specified key.
                        Returns the original dictionary if "element_types" key
                        is not found or if its value is not a list.
    """
    for position in position_data["element_types"]:
        del position["sub_positions_locked"]
    return position_data

    

def create_json_data(file_path: str, file_name_json: str, data: Union[Dict[str, Any], List[Dict[str, Any]]]) -> None:
    """
    Writes data to a file in JSON Lines (JSONL) format. Each item in the data
    (or the data itself if it's a single dictionary) is written as a separate
    JSON object followed by a newline.

    Args:
        file_path (str): The directory path where the file will be created (e.g., "data/output").
        file_name_json (str): The name of the JSON file (e.g., "output.jsonl").
        data (Union[Dict[str, Any], List[Dict[str, Any]]]): The data to write.
            Can be a single dictionary or a list of dictionaries.
    """
    full_file_path = os.path.join(file_path, file_name_json)
    data_to_write = data if isinstance(data, list) else [data]

    with open(full_file_path, "w") as file:
        for item in data_to_write:
            json_line = json.dumps(item)
            file.write(json_line + '\n')
            
    logging.info(f"{file_path} data is created")


def create_blob_directory(local_filepath: str, file_name_json: str, storage_account_url: str, storage_account_container: str) -> bool:
    """
    Uploads a local file to a specified 'landing' directory within an Azure Blob Storage container.

    Args:
        local_filepath (str): The local directory path where the file is located (e.g., "C:/data/output").
        file_name_json (str): The name of the file to upload (e.g., "my_data.json").
        storage_account_url (str): The URL of the Azure Storage account (e.g., "https://youraccount.blob.core.windows.net").
        storage_account_container (str): The name of the target container in Blob Storage (e.g., "raw-data").

    Returns:
        bool: True if the file was successfully uploaded, False otherwise.
    """
    try:
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(storage_account_container)
        landing_file_upload_path = f"landing/{file_name_json}"
        full_local_file_path = os.path.join(local_filepath, file_name_json)
        blob_client = container_client.get_blob_client(landing_file_upload_path)
        with open(full_local_file_path, "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        logging.info(f"File has been uploaded to {landing_file_upload_path}")
        return True
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")

   

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    try:

        ingest_date = req.params.get('ingest_date')

        if not ingest_date:
            return func.HttpResponse(
                "No parameter supplied. Please provide a 'ingest_date' based on the file to be ingested. Date format should be YYYYMMDD",
                status_code=400
            )
        
        storage_account_url = os.getenv("StorageAccountUrl")
        logging.info(f"{storage_account_url}")
        storage_account_container = os.getenv("StorageAccountContainer")
        logging.info(f"{storage_account_container}")
        if not storage_account_url or not storage_account_container:
            raise ValueError("Storage account URL or container not set in environment variables")
        
        url_list = "https://fantasy.premierleague.com/api/bootstrap-static/"
        metadata = ["events_metadata", "team_metadata", "player_metadata", "position_metadata"]
        local_filepath = tempfile.gettempdir()
        data = fetch_data_api(url_list)
        if data:
                zipped_api = zip(metadata, data)
                for attribute_name,data in zipped_api:
                    current_timestamp = convert_timestamp_to_myt_date()
                    file_name_json = f"raw_fpl_{attribute_name}_{ingest_date}_{current_timestamp}.json"
                    create_json_data(local_filepath, file_name_json, data)
                    create_blob_directory(local_filepath, file_name_json, storage_account_url, storage_account_container)
        return func.HttpResponse(f"Data from external API ingested successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)