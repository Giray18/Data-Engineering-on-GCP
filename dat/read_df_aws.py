import pandas as pd
import s3fs

def df_read_aws(storage_name,file_name):
    ''' Returns dataframe by reading defined dataset from defined source'''
    if "csv" in file_name.split("."):
        file_name = file_name.split(".")
        df = pd.read_csv(f"s3://{storage_name}/{file_name[0]}.csv")
    elif "parquet" or "pqt" in file_name.split("."):
        file_name = file_name.split(".")
        df = pd.read_parquet(f"s3://{storage_name}/de/{file_name[0]}.parquet")
    else:
        print("Unavailable file format")
    return df

if __name__ == '__main__':
    df_read_aws()