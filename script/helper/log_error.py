from importlib import reload
from dotenv import load_dotenv
import os
import pandas as pd
import sys
from datetime import datetime
from datetime import timezone
import pandas as pd
from sqlalchemy import create_engine
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.db_conn import db_connection


load_dotenv()

def etl_log(log_msg: dict):
    """
    This function is used to save the log message to the database.
    """


    try:
        _, _, _,log_engine = db_connection()

        df = pd.DataFrame([log_msg])

        #write data log
        df.to_sql('etl_log', 
                    con = log_engine, 
                    if_exists = 'append', 
                    index = False, 
                    schema = 'public')
    except Exception as e:
        print("Can't save your log message. Cause: ", str(e))


def log_error(step,component, table_name,error_msg):
    log_msg = {
        "step" : step,
        "component" : component, 
        "status" : "failed", 
        "table_name": table_name,
        "error_msg": error_msg,
        "etl_date": datetime.now(timezone.utc).astimezone()  # Current timestamp
        }
    etl_log(log_msg)






    

    
    

    