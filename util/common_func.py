from datetime import datetime
import pytz
import os
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient
from azure.keyvault.secrets import SecretClient

def convert_timestamp_to_myt_date():
    current_utc_timestamp = datetime.utcnow()
    utc_timezone = pytz.timezone("UTC")
    myt_timezone = pytz.timezone("Asia/Kuala_Lumpur")
    myt_timestamp = utc_timezone.localize(current_utc_timestamp).astimezone(myt_timezone)
    formatted_timestamp = myt_timestamp.strftime("%H%M%S")
    return formatted_timestamp


def get_secret_value(key_vault_url: str) -> str:
    """
    Get service principal details

    Args:
        key_vault_url str: Azure Key Vault url.
    
    Returns:
        str: Return client id, secret and tenant id value.
    """

    # Create a DefaultAzureCredential object to authenticate with Azure Key Vault
    credential = DefaultAzureCredential(managed_identity_client_id=os.getenv("ManagedIdentityClientId"))

    # Create a SecretClient instance
    secret_client = SecretClient(vault_url=key_vault_url, credential=credential)

    # Get the secret value
    sp_retrieved_secret = secret_client.get_secret(os.getenv("SpSecretName"))

    # Get the client id
    sp_retrieved_client_id = secret_client.get_secret(os.getenv("SpClientId"))

    # Get the tenant id
    sp_retrieved_tenant_id = secret_client.get_secret(os.getenv("SpTenantId"))

    return sp_retrieved_client_id.value, sp_retrieved_secret.value, sp_retrieved_tenant_id.value


def create_storage_options(azure_dev_key_vault_url):
    client_id, client_secret, client_tenant_id = get_secret_value(azure_dev_key_vault_url)

    storage_options = {
        'azure_storage_account_name': os.getenv("StorageAccountName"),
        'azure_storage_client_id': client_id,
        'azure_storage_client_secret': client_secret,
        'azure_storage_tenant_id': client_tenant_id,
    }

    return storage_options