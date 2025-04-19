import pandas as pd
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.db_conn import db_connection
from pangres import upsert
from etl.extract import *
from helper.log_error import log_error
from helper.log_success import log_success

def load_stg(data,table_name):
    step="staging"
    component="load"
    try:
        _, stg_engine, _,_ = db_connection()
        get_key={
            "car_brand":"brand_car_id",
            "car_sales":"id_sales",
            "us_state":"id_state"
        }
        try:
            data = data.drop('created_at', axis=1)
        except:
            pass
        data=data.set_index([get_key.get(table_name)])
        upsert(con=stg_engine, 
                df=data, 
                table_name=table_name, 
                schema="public", 
                if_row_exists="update")
        log_success(step,component,table_name)  
        print (f"Load data to {table_name} is succeed")
    except Exception as e:
        print(f"====== Failed to Load Data {table_name} ======,\n {e}")
        log_error(step,component,table_name,str(e))


def load_dwh(data,table_name):
    step="datawarehouse"
    component="load"  

    try:
        _, _, dwh_engine,_ = db_connection()
        upsert(con=dwh_engine, 
        df=data, 
        table_name=table_name, 
        schema="public", 
        if_row_exists="update")
        print (f"Load data {table_name} to Datawarehouse  is succeed")
    except Exception as e:
        print(f"====== Failed to Load Data {table_name} ====== to Datawarehouse,\n {e}")
        log_error(step,component,table_name,str(e))
    




