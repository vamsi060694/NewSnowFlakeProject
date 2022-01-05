
from snowflake.connector.pandas_tools import write_pandas
from utils import snowflake_connection, downloading_loading, cs
from utils import reading_snowflake_loading_df
from utils import reading_snowflake_loading_df_1
import snowflake.connector
import sys




if len(sys.argv) < 2:
    sql = "select * from LOOK_UP_TABLE "
    look_up = reading_snowflake_loading_df(cs, sql)
    df = look_up.reset_index(drop=True)
    #print(type(df))
    downloading_loading(df)
else:
    for i in range(1,len(sys.argv)):
        sql1 = "select * from LOOK_UP_TABLE where CONTAINER_NAME= ?"
        look_up = reading_snowflake_loading_df_1(cs, sql1,sys.argv[i])
        df = look_up.reset_index(drop=True)
        downloading_loading(df)
cs.close()







