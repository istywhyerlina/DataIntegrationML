import pandas as pd
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4')
from script.helper.db_conn import db_connection
from script.helper.upload_minio import *
from pangres import upsert
from script.etl.extract import *
from script.etl.load import *
from script.etl.transform import *
from script.ML.split import *
from script.ML.preprocessing import *
from sklearn.linear_model import LinearRegression


from sqlalchemy import *

#Extract Data From Source
df1=extract_sheet(key_file, "brand_car")

df2=extract_db("car_sales", "src")

df3=extract_api("https://raw.githubusercontent.com/Kurikulum-Sekolah-Pacmann/us_states_data/refs/heads/main/us_states.json")

#Loading Data To Staging
load_stg(df1, "car_brand")
df2=df2.drop(['model','trim','body','vin','seller','saledate'], axis=1)
load_stg(df2, "car_sales")
load_stg(df3, "us_state")

#Extract Data From Staging
_, stg_engine, dwh_engine,_ = db_connection()
meta = MetaData()
meta.reflect(bind=stg_engine)
for table in meta.sorted_tables:
    globals()[f'{table}']=extract_db(str(table), "stg")

#Transform Data 
car_brand=tf_car_brand(car_brand)
car_sales=tf_car_sales(car_sales)   
us_state=tf_us_state(us_state)  

#Load Data To Data Warehouse    
to_load=['us_state','car_brand','car_sales']
for table in to_load:
    load_dwh(globals()[f'{table}'],table)

#Machine Learning: Preprocessing
df=preprocessing()


column_name=df.columns.tolist()

#Input Missing Value
from sklearn.impute import SimpleImputer
my_imputer = SimpleImputer()
df2 = pd.DataFrame(my_imputer.fit_transform(df))
df2 = df2.set_axis(column_name, axis=1)
X_train, X_test, y_train, y_test = splitting_process(data = df2,
                                                         target_col = 'selling_price',
                                                         test_size = 80)

regressor = LinearRegression()
regressor.fit(X_train, y_train)

score = regressor.score(X_test, y_test)
print('score = '+ str(score))

#Dump Model to Minio
dump_bucket(regressor,"model")



