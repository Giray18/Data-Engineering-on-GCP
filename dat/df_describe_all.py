import pandas as pd
import numpy as np

def df_describe_all(df: pd.DataFrame):
    ''' Returns short/statistical summary of all columns'''
    describe_all_cols = df.describe(include='all')
    return describe_all_cols

if __name__ == '__main__':
    df_describe_all()