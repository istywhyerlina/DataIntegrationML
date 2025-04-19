import pandas as pd
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.db_conn import db_connection
from pangres import upsert
from etl.extract import *
from etl.load import *
from helper.log_error import log_error
from helper.log_success import log_success

from sqlalchemy import *
def tf_car_brand(data):
    try:
        data= data.drop('created_at', axis=1)
        data = data.rename({'brand_car_id': 'brand_car_id_nk'}, axis='columns')
        get_key={
                "car_brand":"brand_car_id_nk",
                "car_sales":"id_sales_nk",
                "us_state":"id_state_nk"
            }
        data=data.set_index([get_key.get('car_brand')], drop = False)
        data= data.drop('brand_car_id_nk', axis=1)
        log_success("datawarehouse","transform","car_brand")  
        return(data)
    except Exception as e:
        print(f"====== Failed to Transform Data car_brand ======,\n {e}")
        log_error("datawarehouse","transform","car_brand",str(e))
def tf_us_state(data):
    try:
        data= data.drop('created_at', axis=1)
        data = data.rename({'id_state': 'id_state_nk'}, axis='columns')    
        get_key={
                    "car_brand":"brand_car_id_nk",
                    "car_sales":"id_sales_nk",
                    "us_state":"id_state_nk"
                }
        data=data.set_index([get_key.get('us_state')], drop = False)
        data= data.drop('id_state_nk', axis=1)
        log_success("datawarehouse","transform","us_state")  
        return data
    except Exception as e:
        print(f"====== Failed to Transform Data us_state ======,\n {e}")
        log_error("datawarehouse","transform","us_state",str(e))

def tf_car_sales(data):
    try:
        _, stg_engine, dwh_engine,_ = db_connection()
        car_brand_dwh=extract_db(str('car_brand'), "dwh")
        car_brand_dwh=car_brand_dwh[['brand_car_id','brand_name']]
        us_state_dwh=extract_db(str('us_state'), "dwh")
        us_state_dwh=us_state_dwh[['id_state','code']]
        data=data.merge(car_brand_dwh, how="left", left_on='brand_car', right_on='brand_name')
        data=data.merge(us_state_dwh, how="left", left_on='state', right_on='code')
        data=data.drop(['brand_car','state','brand_name','code','created_at'], axis=1)
        data = data.rename({'id_sales': 'id_sales_nk', 'sellingprice':'selling_price'}, axis='columns')
        get_key={
                    "car_brand":"brand_car_id_nk",
                    "car_sales":"id_sales_nk",
                    "us_state":"id_state_nk"
                }
        data=data.set_index([get_key.get('car_sales')], drop = False)
        data= data.drop('id_sales_nk', axis=1)
        log_success("datawarehouse","transform","car_sales")  
        return data
    except Exception as e:
        print(f"====== Failed to Transform Data car_sales ======,\n {e}")
        log_error("datawarehouse","transform","car_sales",str(e))