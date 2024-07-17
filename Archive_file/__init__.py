import logging
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func
from datetime import datetime


def list_blob(storage_account_url, container_name, credential):
    blob_service_client = BlobServiceClient(storage_account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)
    blob_list = container_client.list_blobs()

    blob_name = []

    for blob in blob_list:
        blob_name.append(blob.name)

    return blob_name


def get_old_date(list_of_files):
    date_counts = set()
    for path in list_of_files:
        parts = path.split('/')
        if len(parts) > 2 and parts[2].isdigit() and "current" in parts:
            date_counts.add(parts[2])
    if date_counts:
        return min(date_counts)
    return None


def copy_file_to_archive(archive_date, storage_account_url, container_name, list_of_files, credential):
    blob_service_client = BlobServiceClient(storage_account_url, credential=credential)
    container_client = blob_service_client.get_container_client(container_name)

    # Reference - https://stackoverflow.com/questions/32500935/python-how-to-move-or-copy-azure-blob-from-one-container-to-another
    for folder_path in list_of_files:
        folder_path_list = folder_path.split("/")
        if len(folder_path_list) > 1:
              if "current" in folder_path_list and archive_date in folder_path_list and len(folder_path_list) > 3:
                source_blob_path = "/".join(folder_path_list)
                folder_path_list[1] = "archive"
                destination_blob_path = "/".join(folder_path_list)
                source_blob = container_client.get_blob_client(source_blob_path)
                destination_blob = container_client.get_blob_client(destination_blob_path)
                destination_blob.start_copy_from_url(source_blob.url)
                source_blob.delete_blob()


def delete_old_directory(data_lake_url, container_name, archive_date, credential):
    datalake_service_client = DataLakeServiceClient(account_url=data_lake_url, credential=credential)
    file_system_client = datalake_service_client.get_file_system_client(container_name)
    path_list = file_system_client.get_paths()

    # Reference - https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-file-datalake/samples/datalake_samples_file_system.py
    for path in path_list:
        path_name = path.name
        parts = path_name.split("/")
        print(parts)
        if len(parts) > 2 and parts[2] == archive_date and parts[1] == "current":
            parts_path = "/".join(parts)
            print(parts_path)
            file_system_to_delete = file_system_client.get_directory_client(parts_path)
            file_system_to_delete.delete_directory()


def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")

    storage_account_url = os.getenv("StorageAccountUrl")
    data_lake_url = os.getenv("DataLakeUrl")
    storage_account_container = os.getenv("StorageAccountContainer")
    default_credential = DefaultAzureCredential()

    try:
        blob_list = list_blob(storage_account_url, storage_account_container, default_credential)
        oldest_date = get_old_date(blob_list)
        
        if oldest_date:
            copy_file_to_archive(oldest_date, storage_account_url, storage_account_container, blob_list, default_credential)
            delete_old_directory(data_lake_url, storage_account_container, oldest_date, default_credential)
            logging.info(f"File date {oldest_date} is archived.")
            return func.HttpResponse(f"File date {oldest_date} is archived.", status_code=200)
        else:
            logging.info("No need to archive file")
            return func.HttpResponse(f"No need to archive file.", status_code=200)
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)
