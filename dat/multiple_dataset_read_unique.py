import pandas as pd
import os
import glob
import numpy as np

def multiple_dataset_read_unique(path = []):
    ''' This function reads multiple csv files from location passed as parameter 
    then detects unique value holding columns from all datasets located in location '''
    # use glob to get all the csv files 
    # in the folder
    csv_files = glob.glob(os.path.join(path, "*.csv"))

    #xlsx location
    xlsx_files = glob.glob(os.path.join(path, "*.xlsx"))

    # loop over the list of csv files
    df_empty = pd.DataFrame()
    for f in csv_files:
        # read the csv file
        df = pd.read_csv(f, low_memory=False)
        File_name =  f.split("\\")[-1]
        for (columnName, columnData) in df.items():
            #### COUNTS WITHOUT NULL VALUES SO RESULTS ARE TRUE FOR ALL COLUMNS
            if (len(columnData) - (columnData.isna().sum())) == columnData.nunique() and columnData.notnull().any() == True:
                list_req = []
                list_req.extend((File_name,columnName,len(columnData)))
                df_unique_cols = pd.DataFrame([list_req] ,columns = ["File_name","columnName","len(columnData)"])
                df_empty = pd.concat([df_empty,df_unique_cols])
    return df_empty


if __name__ == '__main__':
    multiple_dataset_read_unique()