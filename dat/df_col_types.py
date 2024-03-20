import pandas as pd

def df_col_types(df: pd.DataFrame):
    ''' Returns a dataframe consisting of a list
    covers field names and data types of fields'''
    col_types = df.dtypes
    df_col_types = pd.DataFrame([col_types.values],columns=[df.columns])
    return df_col_types


if __name__ == '__main__':
    df_col_types()