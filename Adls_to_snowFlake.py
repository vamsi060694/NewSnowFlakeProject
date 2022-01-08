from utils import downloading_loading, cs
from utils import look_up_table
from utils import look_up_tabl_1
from utils import init_logger
import sys

logger = init_logger()

if len(sys.argv) < 2:
    try:
        sql = "select * from LOOK_UP_TABLE "
        look_up = look_up_table(cs, sql)
        df = look_up.reset_index(drop=True)
        downloading_loading(df)
    except Exception as e:
        logger.error(e)

else:
    try:
        for i in range(1, len(sys.argv)):
            sql1 = "select * from LOOK_UP_TABLE where CONTAINER_NAME= ?"
            look_up = look_up_tabl_1(cs, sql1, sys.argv[i])
            df = look_up.reset_index(drop=True)
            downloading_loading(df)
    except Exception as e:
        logger.error(e)

cs.close()
