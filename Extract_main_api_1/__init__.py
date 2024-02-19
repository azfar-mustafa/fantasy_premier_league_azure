import logging
import tempfile
import azure.functions as func
import requests
import pytz
import json
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient



def fetch_data(website_url):
    try:
        player_team_detail_url = website_url
        response = requests.get(player_team_detail_url, stream=True, timeout=2)
        response.raise_for_status()
        data = response.json()
        return data['events'], data['teams'], data['elements'], data['element_types']
    except Exception as e:
        logging.error(f"An error occured: {e}")
        return None
    

def create_data(file_path, file_name_json, data):
    with open(f"{file_path}/{file_name_json}", 'w') as file:
        json.dump(data, file, indent=4)
    logging.info(f"{file_path} data is created")


def convert_timestamp_to_myt_date():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


def create_blob_directory(local_filepath, file_name_json, attribute_name, current_date, storage_account_url, storage_account_container):
    try:
        default_credential = DefaultAzureCredential()
        blob_service_client = BlobServiceClient(storage_account_url, credential=default_credential)
        container_client = blob_service_client.get_container_client(storage_account_container)
        blob_client = container_client.get_blob_client(f"{attribute_name}/current/{current_date}/{file_name_json}")
        with open(f"{local_filepath}/{file_name_json}", "rb") as data:
            blob_client.upload_blob(data, overwrite=True)
        return True
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        return False

   

def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    try:
        storage_account_url = "https://azfarsadev.blob.core.windows.net"
        storage_account_container = 'bronze'
        url_list = 'https://fantasy.premierleague.com/api/bootstrap-static/'
        metadata = ['events_metadata', 'teams_metadata', 'player_metadata', 'position_metadata']
        current_date = convert_timestamp_to_myt_date()
        local_filepath = tempfile.gettempdir()
        data = fetch_data(url_list)
        if data:
                zipped_api = zip(metadata, data)
                for attribute_name,data in zipped_api:
                    file_name_json = f"{attribute_name}_{current_date}.json"
                    create_data(local_filepath, file_name_json, data)
                    create_blob_directory(local_filepath, file_name_json, attribute_name, current_date, storage_account_url, storage_account_container)
        return func.HttpResponse(f"Data from external API ingested successfully.", status_code=200)
    except Exception as e:
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)