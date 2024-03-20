import pandas as pd
import os
import glob
import numpy as np
import dat
from datetime import datetime, timedelta, date

def json_dataset_read(path = []):
    ''' This function reads multiple json files from location passed as parameter 
    then runs all functions on dat package '''
    # use glob to get all the json files 
    # in the folder
    json_files = glob.glob(os.path.join(path, "*.json"))


    # loop over the list of csv files
    for f in json_files:
        # read the json files
        df = pd.read_json(f)
        if len(df.columns) > len(df.index):
            df = df.transpose()
        else:
            df = df
        File_name =  f.split("\\")[-1]
        # Creating dataframes defined on analysis_dict.py file
        for key,value in dat.analysis_dict().items():
            vars()[key] = value(df)
            # Saving dataframes consisting of analysis into a single excel file
            dat.save_dataframe_excel(vars(),f"analysis_{File_name}_{date.today()}")
    return "analysis files saved"


# if __name__ == '__main__':
#     json_dataset_read()