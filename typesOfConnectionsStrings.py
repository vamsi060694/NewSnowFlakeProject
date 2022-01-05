# #pip install azure-storage-file-datalake
#
import os, uuid, sys
from azure.storage.filedatalake import DataLakeServiceClient
from azure.core._match_conditions import MatchConditions
from azure.storage.filedatalake._models import ContentSettings
from azure.storage.filedatalake import DataLakeServiceClient
from azure.storage.filedatalake import FileSystemClient

#
#
# def initialize_storage_account(storage_account_name, storage_account_key):
#     try:
#         global service_client
#
#         service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
#             "https", storage_account_name), credential=storage_account_key)
#
#     except Exception as e:
#         print(e)
#
# initialize_storage_account('projectgen2lake','mV8phW3sSwhrCct//+Dbd7ijnMAIeGakH+M6FU/aKtWSHdKq/m5KBvWWtaRD9B6mbrPezuBp6TuJ9mpmIoM7Og==')
# def list_directory_contents():
#     try:
#         file_system_client = service_client.get_file_system_client(file_system="project")
#
#         paths = file_system_client.get_paths(path="primary")
#
#         for path in paths:
#             print(path.name + '\n')
#     except Exception as e:
#         print(e)
# list_directory_contents()

# type_2 connection_string
account_url='DefaultEndpointsProtocol=https;AccountName=projectgen2lake;AccountKey=mV8phW3sSwhrCct//+Dbd7ijnMAIeGakH+M6FU/aKtWSHdKq/m5KBvWWtaRD9B6mbrPezuBp6TuJ9mpmIoM7Og==;EndpointSuffix=core.windows.net'
service = DataLakeServiceClient.from_connection_string(conn_str=account_url)

file_system = FileSystemClient.from_connection_string(account_url, file_system_name="project")

paths = file_system.get_paths()
for path in paths:
    print(path.name + '\n')


























