from typing import Tuple
from sklearn.model_selection import train_test_split
import pandas as pd
import sys
sys.path.insert(0, '/home/istywhyerlina/PipelinePyspark/exercise4/data_pipeline_exercise_4/script')
from helper.log_error import log_error
from helper.log_success import log_success



def splitting_process(
    data: pd.DataFrame, target_col: str, test_size: float
) -> Tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """
    Function that will split the train and test into 80:20 proportion

    Parameters
    ----------
    data (pd.DataFrame): data telco that split into training and testing

    Returns
    -------
    X_train (pd.DataFrame): features data for training data
    X_test (pd.DataFrame): features data for test data
    y_train (pd.Series): target for training data
    y_test (pd.Series): target for test data
    """
    step,component,table_name= "Modelling","Splitting Data","car_sales"
    try:
        # determine the features and the target
        X = data.drop([target_col], axis=1)
        y = data[target_col]


        SEED = 42

        X_train, X_test, y_train, y_test = train_test_split(
            X, y, test_size = test_size, random_state = SEED
        )

        # validate the shape for train and test data
        log_success(step,component,table_name)  
        print("Splitting Succeeded")

        return X_train, X_test, y_train, y_test
    except Exception as e:
        print(f"====== Failed Splitting Data ======,\n {e}")
        log_error(step,component,table_name,str(e))