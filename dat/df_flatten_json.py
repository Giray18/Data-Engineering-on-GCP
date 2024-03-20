import pandas as pd
import json
import re

def df_flatten_json(df: pd.DataFrame):
    ''' Flattenning JSON column and concating it to original 
    df dataframe '''
    regex_json = re.compile(r'[{\[]{1}([,:{}\[\]0-9.\-+Eaeflnr-u \n\r\t]|".*?")+[}\]]{1}', re.IGNORECASE)
    col_with_json_val = [df[i].name for i in df.columns if regex_json.match(str(df[i].iloc[0]))]
    # Below comment out part was being used before
    # col_with_json_val = [df[i].name for i in df.columns if "{" in str(df[i].iloc[0]) and ":" in str(df[i].iloc[0]) and "[" not in str(df[i].iloc[0])]
    df_json_cols = pd.DataFrame(col_with_json_val,columns = ["col_with_json_val"])
    if int(df_json_cols.size) > 0:
        for i in df_json_cols.col_with_json_val.values:
            vars()[i] = df[i].map(lambda x: json.loads(x))
            vars()[i] = pd.json_normalize(vars()[i])
            df = df.join(vars()[i])
    else:
        df
    return df


if __name__ == '__main__':
    df_flatten_json()