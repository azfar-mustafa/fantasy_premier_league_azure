import logging
import tempfile
import azure.functions as func
import requests
import pytz
import json
from datetime import datetime
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Python HTTP trigger function processed a request.')
    data_to_pass = [1,2,3]
    json_data = json.dumps(data_to_pass)
    return func.HttpResponse(json_data, mimetype="application/json")