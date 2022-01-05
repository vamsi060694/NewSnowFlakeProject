import os
import pandas as pd
import snowflake
from azure.storage.filedatalake import DataLakeFileClient
from snowflake.connector.pandas_tools import write_pandas

snowflake.connector.paramstyle = '?'


def downloading_gen2lake(container_name, file_name):
    account_url = 'DefaultEndpointsProtocol=https;AccountName=projectgen2lake;AccountKey=mV8phW3sSwhrCct//+Dbd7ijnMAIeGakH+M6FU/aKtWSHdKq/m5KBvWWtaRD9B6mbrPezuBp6TuJ9mpmIoM7Og==;EndpointSuffix=core.windows.net'
    file = DataLakeFileClient.from_connection_string(account_url, file_system_name=container_name, file_path=file_name)
    print(container_name, file_name)
    with open(r'C:\Users\Ant PC\PycharmProjects\NewSnowFlakeProject1\{}'.format(file_name), "wb") as my_file:
        download = file.download_file()
        download.readinto(my_file)
    return my_file


def reading_snowflake_loading_df(cs, sql):
    cs.execute(sql)
    df = cs.fetch_pandas_all()
    return df


def reading_snowflake_loading_df_1(cs, sql, a):
    cs.execute(sql, [a])
    df = cs.fetch_pandas_all()
    return df


def snowflake_connection():
    ctx = snowflake.connector.connect(
        user=access_details['user'],
        password=access_details['password'],
        account=access_details['account'],
        warehouse=access_details['warehouse'],
        database=access_details['database'],
        schema=access_details['schema']
    )
    return ctx


access_details = {'user': 'mysnowflake94',
                  'password': 'Vam1347gmc',
                  'account': 'kn94297.central-us.azure',
                  'warehouse': 'COMPUTE_WH',
                  'database': 'project',
                  'schema': 'public'}


def downloading_loading(x):
    for row in x.values:
        downloaded_file = downloading_gen2lake(row[0], row[1])
        path = r'C:\Users\Ant PC\PycharmProjects\NewSnowFlakeProject1\{}'.format(row[1])
        vendor_data_df = pd.read_csv(path, header=0)
        vendor_data_df.reset_index(drop=True, inplace=True)
        success, nchunks, nrows, _ = write_pandas(y, vendor_data_df, row[2])
        print(success)
        if os.path.exists(path):
            os.remove(path)
        else:
            print("The file does not exist")
    return


y = snowflake_connection()
cs = y.cursor()
