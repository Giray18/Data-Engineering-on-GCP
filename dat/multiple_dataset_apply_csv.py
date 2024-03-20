import pandas as pd
import os
import glob
import numpy as np
import dat
from datetime import datetime, timedelta, date

def multiple_dataset_apply(path = []):
    ''' This function reads multiple csv files from location passed as parameter 
    then runs all functions on dat package '''
    # use glob to get all the csv files 
    # in the folder
    csv_files = glob.glob(os.path.join(path, "*.csv"))

    #xlsx location
    xlsx_files = glob.glob(os.path.join(path, "*.xlsx"))

    # loop over the list of csv files
    for f in csv_files:
        # read the csv files
        df = pd.read_csv(f, low_memory=False)
        File_name =  f.split("\\")[-1]
        # Creating dataframes defined on analysis_dict.py file
        for key,value in dat.analysis_dict().items():
            vars()[key] = value(df)
            # Saving dataframes consisting of analysis into a single excel file
            dat.save_dataframe_excel(vars(),f"analysis_{File_name}_{date.today()}")
    return "dataset_analysis_saved"


if __name__ == '__main__':
    multiple_dataset_apply()