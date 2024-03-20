import pandas as pd

def df_detect_json(df: pd.DataFrame):
    ''' Returns a dataframe consisting of field names 
    that has JSON format in it'''
    col_with_json_val = [df[i].name for i in df.columns if "{" in str(df[i].iloc[0]) and ":" in str(df[i].iloc[0])]
    df_json_cols = pd.DataFrame(col_with_json_val,columns = ["col_with_json_val"])
    return df_json_cols


if __name__ == '__main__':
    df_detect_json()