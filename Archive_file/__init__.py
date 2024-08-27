import logging
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func


def copy_file_to_archive(service_client, container_name: str, data_source_type, current_date, file_name):
    try:
        file_system_client = service_client.get_file_system_client(container_name)
        source_path = f"landing/{data_source_type}/current/{current_date}/{file_name}"
        destination_path = f"landing/{data_source_type}/archive/{current_date}/{file_name}"
        source_file = file_system_client.get_file_client(source_path)
        dest_file = file_system_client.get_file_client(destination_path)
        if not source_file.exists():
            logging.info(f"There is no file in {data_source_type} to be archived")
        else:
            dest_file.upload_data(source_file.download_file().readall(), overwrite=True)
            source_file.delete_file()
            logging.info(f"File {file_name} copied to archive and deleted from current folder")
    ## Reference - https://stackoverflow.com/questions/32500935/python-how-to-move-or-copy-azure-blob-from-one-container-to-another
    except Exception as e:
        logging.error(f"An error occurred while copying and deleting the file: {str(e)}")


def delete_old_directory(datalake_service_client, container_name, archive_date, data_source_type):
    file_system_client = datalake_service_client.get_file_system_client(container_name)
    path_list = file_system_client.get_paths()

    # Reference - https://github.com/Azure/azure-sdk-for-python/blob/main/sdk/storage/azure-storage-file-datalake/samples/datalake_samples_file_system.py
    for path in path_list:
        path_name = path.name
        parts = path_name.split("/")
        if len(parts) > 3 and parts[2] == "current" and parts[3] == archive_date and parts[1] == data_source_type and parts[0] == "landing":
            parts_path = "/".join(parts)
            logging.info(f"Folder to be deleted - {parts_path}")
            file_system_to_delete = file_system_client.get_directory_client(parts_path)
            file_system_to_delete.delete_directory()



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
    
    data_lake_url = os.getenv("DataLakeUrl")
    storage_account_container = os.getenv("StorageAccountContainer")
    default_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=data_lake_url, credential=default_credential)

    file_name = f'{data_source_type}_{file_date}.json'

    try:
        copy_file_to_archive(service_client, storage_account_container, data_source_type, file_date, file_name)
        delete_old_directory(service_client, storage_account_container, file_date, data_source_type)
        return func.HttpResponse(f"Process Completed.", status_code=200)
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)
