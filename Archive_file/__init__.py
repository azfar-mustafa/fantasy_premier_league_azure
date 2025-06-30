import logging
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.storage.filedatalake import DataLakeServiceClient
import azure.functions as func


def copy_file_to_archive(service_client: str, container_name: str, file_name: str) -> None:
    """
    Process of archiving the source files from landing to archive folder.

    Args:
        service_client (str): The credential to access ADLS2.
        container_name (str): Container in ADLS2 that stores the source files.
        file_name (str): The source file name.
    """
    try:
        file_system_client = service_client.get_file_system_client(container_name)
        for file in file_name:
            source_path = f"landing/{file}"
            destination_path = f"archive/{file}"
            source_file = file_system_client.get_file_client(source_path)
            dest_file = file_system_client.get_file_client(destination_path)
            if not source_file.exists():
                logging.info(f"There is no file to be archived")
            else:
                dest_file.upload_data(source_file.download_file().readall(), overwrite=True)
                source_file.delete_file()
                logging.info(f"File {file_name} copied to archive and deleted from current folder")
    ## Reference - https://stackoverflow.com/questions/32500935/python-how-to-move-or-copy-azure-blob-from-one-container-to-another
    except Exception as e:
        logging.error(f"An error occurred while copying and deleting the file: {str(e)}")


def list_directory_contents(service_client: str, container_name: str, directory_name: str) -> list:
    """
    List source file to be archive

    Args:
        service_client (str): The credential to access ADLS2.
        container_name (str): Container in ADLS2 that stores the source files.
        directory_name (str): The path for source file in landing folder.
    Returns:
        list: Return a list contain source files name.
    """
    file_list = []
    file_system_client = service_client.get_file_system_client(container_name)
    paths = file_system_client.get_paths(path=directory_name)

    file_type = 'raw_fpl_'

    for path in paths:
        file_name = path.name.removeprefix("landing/")
        if file_type in file_name:
            file_list.append(file_name)

    if not file_list:
        logging.info(f"No files in landing folder. Skipping all process.")
        return None
    
    logging.info(f"{file_list}")

    return file_list



def main(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Python HTTP trigger function processed a request.")
    
    data_lake_url = os.getenv("DataLakeUrl")
    storage_account_container = os.getenv("StorageAccountContainer")
    default_credential = DefaultAzureCredential()
    service_client = DataLakeServiceClient(account_url=data_lake_url, credential=default_credential)

    try:
        file_to_be_archive = list_directory_contents(service_client, storage_account_container, 'landing/')
        if file_to_be_archive is None:
            logging.info("Main function stopping early. No files found to be archived")
            return func.HttpResponse(f"Archive process completed. No files found to be archived.", status_code=200)
        
        copy_file_to_archive(service_client, storage_account_container, file_to_be_archive)
        return func.HttpResponse(f"Archive process completed.", status_code=200)
    except Exception as e:
        logging.error(f"An error occured: {str(e)}")
        return func.HttpResponse(f"An error occured: {str(e)}", status_code=500)
