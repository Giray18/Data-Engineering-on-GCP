import pandas as pd
from azure.storage.blob import BlobServiceClient, generate_account_sas, ResourceTypes, AccountSasPermissions
from datetime import datetime, timedelta, date
import dat


def create_account_sas_container(account_name: str, account_key: str):
    ''' Creates SAS token for container level operations '''
    # Create an account SAS that's valid for one day
    start_time = datetime.utcnow()
    expiry_time = start_time + timedelta(days=1)

    # Define the SAS token permissions
    sas_permissions=AccountSasPermissions(list=True)

    # Define the SAS token resource types
    # For this example, we grant access to service-level APIs
    sas_resource_types=ResourceTypes(container=True)

    sas_token_container = generate_account_sas(
        account_name=account_name,
        account_key=account_key,
        resource_types=sas_resource_types,
        permission=sas_permissions,
        expiry=expiry_time,
        start=start_time
    )

    return sas_token_container



def df_read_azure_multiple_files(storage_account_name: str, account_key: str, container_name: str):
    ''' Returns dataframes by reading all datasets on mentioned container and applies
    needed data analysis methods on dataset and saves them to working directory '''

    # Creating service client var 
    blob_service_client = BlobServiceClient(account_url=f"https://{storage_account_name}.blob.core.windows.net"
                                            , credential=create_account_sas_container(storage_account_name,account_key))
    container_client = blob_service_client.get_container_client(container_name)
    
    blob_list = container_client.list_blobs()
    for blob in blob_list:
        current_blob = blob.name
        source = f"https://{storage_account_name}.blob.core.windows.net/{container_name}/" + current_blob
        df = pd.read_csv(source)
        File_name =  current_blob
        # Creating dataframes defined on analysis_dict.py file
        for key,value in dat.analysis_dict().items():
            vars()[key] = value(df)
            # Saving dataframes consisting of analysis into a single excel file
            dat.save_dataframe_excel(vars(),f"analysis_{File_name}_{date.today()}")
    return "dataset_analysis_saved"