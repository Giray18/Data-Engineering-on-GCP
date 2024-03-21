import pandas as pd
import os
import glob
import numpy as np
import dat
from datetime import datetime, timedelta, date

def read_df_json(path = []):
    ''' This function reads multiple json files from location passed as parameter 
    and transposes them by a condition '''
    # use glob to get all the json files 
    # in the folder
    # json_files = glob.glob(os.path.join(path, "*.json"))
    # # loop over the list of json files
    # for f in json_files:
    #     print(f)
    # read the json files
    df = pd.read_json(path)
    if len(df.columns) > len(df.index):
        df = df.transpose()
    else:
        df
    game_ids = df.index.to_list()
    col_with_json_val = [df[i].name for i in df.columns if "{" in str(df[i].iloc[0]) and ":" in str(df[i].iloc[0])]
    for col in df.columns:
        if df[col].dtype == "object" and df[col].name not in col_with_json_val:
            try:
                df[col] = [i.lower() for i in df[col]]
                # df[col] = [i.replace('{', "[") for i in df[col]]
                # df[col] = [i.replace('}', "]") for i in df[col]]
                df[col] = [i.replace('&', "and") for i in df[col]]
            except AttributeError:
                # pass
                for c in game_ids:
                    df[col][c] =  [v.lower() for v in df[col][c]]
                    df[col][c] =  [v.replace('&', "and") for v in df[col][c]]
        # else:
        #     df[col] = [i.replace('{', "[") for i in df[col]]
        #     df[col] = [i.replace('}', "]") for i in df[col]]
    # df = df.to_json("/home/vforvalbuena/GCP_AIRFLOW/newline.json",orient="records",lines=True)
    df = df.to_json(orient="records",lines=True)
    return df

if __name__ == '__main__':
    read_df_json()