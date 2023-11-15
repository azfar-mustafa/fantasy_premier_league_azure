import logging

import azure.functions as func
import os
import configparser
import time
import requests
import pytz
import json
import shutil


def configure_logging():
    log_format = "%(asctime)s - %(levelname)s - %(filename)s - %(message)s"
    log_file_current_timestamp = time.strftime("%Y%m%d")
    log_folder_path = "C:/Users/khair/project/fantasy_premier_league/log/"
    log_file_name = f"fantasy_premier_league_{log_file_current_timestamp}.log"
    log_file_path = f"{log_folder_path}{log_file_name}"
    logging.basicConfig(filename=log_file_path, encoding='utf-8', level=logging.INFO, format=log_format)


def get_file_path():
    config = configparser.ConfigParser()
    config.read('C:/Users/khair/project/fantasy_premier_league/config/config.ini')
    bronze_folder = config['file_path']['bronzefolder']
    return bronze_folder


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
    with open(f"{file_path}\{file_name_json}", 'w') as file:
        json.dump(data, file, indent=4)
    logging.info(f"{file_path} data is created")


def convert_timestamp_to_myt():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone('UTC')
    myt_timezone = pytz.timezone('Asia/Kuala_Lumpur')
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%d%m%Y")
    return formatted_timestamp


def create_directory(folder_timestamp, data_type, bronze_folder):
    folder_path = os.path.join(bronze_folder, data_type, folder_timestamp)
    if os.path.exists(folder_path):
        shutil.rmtree(folder_path)
        logging.info(f"Existing {folder_path} is deleted")
    os.makedirs(folder_path)
    return folder_path



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')

    url_list = 'https://fantasy.premierleague.com/api/bootstrap-static/'

    #name = req.params.get('name')
    #if not name:
    #    try:
    #        req_body = req.get_json()
    #    except ValueError:
    #        pass
    #    else:
    #        name = req_body.get('name')
#
    #if name:
    #    data = fetch_data(url_list)
    #    return func.HttpResponse(f"{data}")
    #else:
    #    return func.HttpResponse(
    #         "This HTTP triggered function executed successfully. Pass a name in the query string or in the request body for a personalized response.",
    #         status_code=200
    #    )
    data = fetch_data(url_list)
    return func.HttpResponse(f"Data from external API ingested successfully. {len(data)} records processed.", status_code=200)
