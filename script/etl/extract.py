from oauth2client.service_account import ServiceAccountCredentials
import gspread
import pandas as pd
from dotenv import load_dotenv
import os
import csv
from datetime import datetime
import requests
import time
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.read_sql import read_sql_file
from helper.db_conn import db_connection
from helper.log_error import log_error
from helper.log_success import log_success


import os



load_dotenv()

CRED_PATH = os.getenv("CRED_PATH")
key_file=os.getenv("KEY_FILE")

def auth_gspread():
    scope = ['https://spreadsheets.google.com/feeds',
             'https://www.googleapis.com/auth/drive']

    #Define your credentials
    credentials = ServiceAccountCredentials.from_json_keyfile_name(CRED_PATH, scope) # Your json file here

    gc = gspread.authorize(credentials)

    return gc

def init_key_file(key_file:str):
    #define credentials to open the file
    gc = auth_gspread()

    #open spreadsheet file by key
    sheet_result = gc.open_by_key(key_file)

    return sheet_result

def extract_sheet(key_file:str, worksheet_name: str) -> pd.DataFrame:
    step="source"
    component="extract"
    try:
        # init sheet
        sheet_result = init_key_file(key_file)

        # Access the specified worksheet within the spreadsheet
        worksheet_result = sheet_result.worksheet(worksheet_name)

        # Retrieve all values from the worksheet and create a DataFrame
        df_result = pd.DataFrame(worksheet_result.get_all_values())

        # set first rows as columns
        df_result.columns = df_result.iloc[0]

        # get all the rest of the values
        df_result = df_result[1:].copy()
        table_name="Brand Car Spreadsheet"
        log_success(step,component,table_name)
        print(f"Extract Data {table_name} from Source is Succeed")

        return df_result
    except Exception as e:
        print(f"====== Failed to Extract Data {key_file} ======,\n {e}")
        log_error(step,component,key_file,str(e))


def extract_db(table,from_db: "src"):
    try:
        src_engine, stg_engine, dwh_engine,_ = db_connection()
        src_conn=src_engine.connect()
        stg_conn=stg_engine.connect()
        dwh_conn=dwh_engine.connect()
        extract_query = "SELECT * FROM {table_name};"
        if from_db =="src":
            step="source"
            component="extract"
            df = pd.read_sql_query(extract_query.format(table_name = table), con=src_conn)
            log_success(step,component,table)
            print(f"Extract Data {table} from Source is Succeed")
            return df
        elif from_db =="stg":
            step="staging"
            component="extract"            
            df = pd.read_sql_query(extract_query.format(table_name = table), con=stg_conn)
            log_success(step,component,table)
            print(f"Extract Data {table} from Staging is Succeed")
            return df
        elif from_db =="dwh":
            step="dwh"
            component="extract"    
            df = pd.read_sql_query(extract_query.format(table_name = table), con=dwh_conn)        
            log_success(step,component,table)
            print(f"Extract Data {table} from Datawarehouse is Succeed")
            return df
        
    except Exception as e:
        print(f"====== Failed to Extract Data {table} ======,\n {e}")
        log_error(step,component,table,str(e))

def extract_api(link):
    try:
        # Establish connection to API
        resp = requests.get(link)

        # Parse the response JSON
        raw_response = resp.json()

        # Convert the JSON data to a pandas DataFrame
        df_api = pd.DataFrame(raw_response['regions'])
        step="source"
        component="extract"
        log_success(step,component,"states")
        print(f"Extract Data us_state from API source is Succeed")
        

        return df_api
    except Exception as e:
        print(f"====== Failed to Extract Data states ======,\n {e}")
        log_error(step,component,"states",str(e))






    

