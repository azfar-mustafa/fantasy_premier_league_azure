import logging
import os
import tempfile
import azure.functions as func
import requests
import json
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from util.common_func import convert_timestamp_to_myt_date


def fetch_data_api(website_url):
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
    

def remove_key_in_position(position_data):
    for position in position_data["element_types"]:
        del position["sub_positions_locked"]
    return position_data

    

def create_json_data(file_path, file_name_json, data):
    data_to_write = data if isinstance(data, list) else [data]

    with open(f"{file_path}/{file_name_json}", "w") as file:
        for item in data_to_write:
            json_line = json.dumps(item)
            file.write(json_line + '\n')
            
    logging.info(f"{file_path} data is created")


def create_blob_directory(local_filepath, file_name_json, attribute_name, current_date, storage_account_url, storage_account_container):
    try:
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(storage_account_container)
        blob_client = container_client.get_blob_client(f"landing/{attribute_name}/current/{current_date}/{file_name_json}")
        with open(f"{local_filepath}/{file_name_json}", "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return True
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")

   

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
                    file_name_json = f"{attribute_name}_{ingest_date}.json"
                    create_json_data(local_filepath, file_name_json, data)
                    create_blob_directory(local_filepath, file_name_json, attribute_name, ingest_date, storage_account_url, storage_account_container)
        return func.HttpResponse(f"Data from external API ingested successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)