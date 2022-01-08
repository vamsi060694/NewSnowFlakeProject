import logging

import snowflake
from snowflake.connector.pandas_tools import write_pandas

snowflake.connector.paramstyle = '?'


def init_logger():
    logging.basicConfig(filename='log_file', format='%(asctime)s : %(levelname)s : %(message)s')
    logger = logging.getLogger(__name__)
    logger.setLevel(logging.INFO)
    logger.setLevel(logging.DEBUG)
    return logger


logger = init_logger()


def snowflake_connection():
    global logger
    logger.info('Intiating snowFlake connection')
    try:
        ctx = snowflake.connector.connect(
            user=access_details['user'],
            password=access_details['password'],
            account=access_details['account'],
            warehouse=access_details['warehouse'],
            database=access_details['database'],
            schema=access_details['schema']
        )
        logger.info("connection Successful")
        logger.info('user:{} connected'.format(access_details['user']))
        return ctx
    except Exception as e:
        logger.error(e)


access_details = {'user': 'mysnowflake94',
                  'password': 'Vam1347gmc',
                  'account': 'kn94297.central-us.azure',
                  'warehouse': 'COMPUTE_WH',
                  'database': 'project',
                  'schema': 'public'}

snowflake_connection()
