import sys
import pickle
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.log_error import log_error
from helper.log_success import log_success
from minio import Minio

from io import BytesIO
from datetime import datetime
from dotenv import load_dotenv
import os
# Load the environment variables
load_dotenv(".env")

# Get the Minio environment variables
ACCESS_KEY_MINIO = os.getenv("MINIO_ACCESS_KEY")
SECRET_KEY_MINIO = os.getenv("MINIO_SECRET_KEY")
def dump_bucket(data,bucket_name):
    try:
        """
        This function is used to handle error or invalid data by uploading the DataFrame to a MinIO bucket.
        """
        current_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        # Initialize MinIO client
        client = Minio('localhost:9001',
                    access_key=ACCESS_KEY_MINIO,
                    secret_key=SECRET_KEY_MINIO,
                    secure=False)
        print("Connect Minio client")

        # Make a bucket if it doesn't exist
        if not client.bucket_exists(bucket_name):
            client.make_bucket(bucket_name)
        print("Create bucket")

        bytes_file = pickle.dumps(data)


        # Upload the CSV file to the bucket
        client.put_object(
            bucket_name=bucket_name,
            object_name="regresi.pkl", #name the fail source name and current etl date
            data=BytesIO(bytes_file),
            length=len(bytes_file),
            content_type='application/pkl'
        )

        # List objects in the bucket
        objects = client.list_objects(bucket_name, recursive=True)
        for obj in objects:
            print(obj.object_name)
        log_success("Modelling","Upload Minio","Pickle File")  
        print("dump Object to bucket")
        
    except Exception as e:
        print(f"====== Failed to Upload Data to MinIO ======,\n {e}")
        log_error("Modelling","Upload Minio","Pickle File",str(e))