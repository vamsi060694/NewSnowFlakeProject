import logging
import os
import pandas as pd
import snowflake

from azure.storage.filedatalake import DataLakeFileClient
from snowflake.connector.pandas_tools import write_pandas

snowflake.connector.paramstyle = '?'

def init_logger():
    logging.basicConfig(filename='log_file', format='%(asctime)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    return logger

logger = init_logger()

def downloading_gen2lake(container_name, file_name):
    try:
        account_url = 'DefaultEndpointsProtocol=https;AccountName=projectgen2lake;AccountKey=mV8phW3sSwhrCct//+Dbd7ijnMAIeGakH+M6FU/aKtWSHdKq/m5KBvWWtaRD9B6mbrPezuBp6TuJ9mpmIoM7Og==;EndpointSuffix=core.windows.net'
        file = DataLakeFileClient.from_connection_string(account_url, file_system_name=container_name,
                                                         file_path=file_name)
        logger.info('container_name:{},file_name:{}'.format(container_name, file_name))
        with open(r'C:\Users\Ant PC\PycharmProjects\NewSnowFlakeProject1\{}'.format(file_name), "wb") as my_file:
            download = file.download_file()
            download.readinto(my_file)
            logger.info('{} downloaded from Gen2Lake'.format(row[1]))
        return my_file
    except Exception as e:
        logger.error(e)


def look_up_table(cs, sql):
    try:
        cs.execute(sql)
        df = cs.fetch_pandas_all()
        return df
    except Exception as e:
        logger.error(e)


def look_up_tabl_1(cs, sql, a):
    try:
        cs.execute(sql, [a])
        df = cs.fetch_pandas_all()
        return df
    except Exception as e:
        logger.error(e)


def snowflake_connection():
    global logger
    try:
        logger.info('Initiating snowFlake Connection')
        ctx = snowflake.connector.connect(
            user=access_details['user'],
            password=access_details['password'],
            account=access_details['account'],
            warehouse=access_details['warehouse'],
            database=access_details['database'],
            schema=access_details['schema']
        )
        logger.info('Connection successful')
        return ctx
    except Exception as e:
        logger.error(e)


access_details = {'user': 'mysnowflake94',
                  'password': 'Vam1347gmc',
                  'account': 'kn94297.central-us.azure',
                  'warehouse': 'COMPUTE_WH',
                  'database': 'project',
                  'schema': 'public'}


def downloading_loading(x):
    global row
    try:
        for row in x.values:
            downloading_gen2lake(row[0], row[1])
            path = r'C:\Users\Ant PC\PycharmProjects\NewSnowFlakeProject1\{}'.format(row[1])
            file_df = pd.read_csv(path, header=0)
            row_count = file_df.shape[0]
            vendor_data_df = pd.read_csv(path, header=0)
            vendor_data_df.reset_index(drop=True, inplace=True)
            success, nchunks, nrows, _ = write_pandas(y, vendor_data_df, row[2])
            logger.info('{} loaded into snowFlake '.format(row[1]))
            if row_count == nrows:
                logger.info('{}:All rows loaded into SnowFlake'.format(row[1]))
            else:
                logger.info('{}:few rows are missing in SnowFlake')
            if os.path.exists(path):
                os.remove(path)
                logger.info('{}:Removed from Local'.format(row[1]))
            else:
                logger.info("The file does not exist")
        return
    except Exception as e:
        logger.error(e)



y = snowflake_connection()
cs = y.cursor()
