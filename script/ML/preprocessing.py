import pandas as pd
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.db_conn import db_connection
from pangres import upsert
from etl.extract import *
from etl.load import *
from etl.transform import *
from ML.split import *

from sqlalchemy import *

def preprocessing():
    step,component,table_name= "Modelling","Prerpocessing Data","car_sales"
    try:
        _, stg_engine, dwh_engine,_ = db_connection()
        meta = MetaData()
        meta.reflect(bind=stg_engine)
        for table in meta.sorted_tables:
            globals()[f'{table}']=extract_db(str(table), "dwh")
        
        car_brand2=car_brand[['brand_car_id','brand_name']]
        us_state2=us_state[['id_state','name']]
        car_sales2 =car_sales .merge(car_brand2, how="left", on='brand_car_id')
        car_sales2 = car_sales2.merge(us_state2, how="left", left_on='id_state', right_on='id_state')
        car_sales2=car_sales2.drop(['brand_car_id','id_state','id_sales','id_sales_nk','created_at'], axis=1)
        car_sales2 = car_sales2.rename({'name': 'state'}, axis='columns')

        #Perform one-hot encoding
        car_sales2 = pd.get_dummies(car_sales2, columns=['transmission','color','interior','brand_name','state'])
        log_success(step,component,table_name)  
        print("Preprocessing Succeeded")
        return car_sales2
    except Exception as e:
        print(f"====== Failed to Preprocessing Data ======,\n {e}")
        log_error(step,component,table_name,str(e))

