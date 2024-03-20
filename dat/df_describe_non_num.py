import pandas as pd
import numpy as np

def df_describe_non_num(df: pd.DataFrame):
    ''' Returns short/statistical summary of non-numerical columns'''
    describe_non_numerical = df.select_dtypes(exclude=[np.number])
    if int(describe_non_numerical.size) > 0:
        describe_non_numerical = describe_non_numerical.describe(exclude=[np.number])
    else:
        describe_non_numerical = pd.DataFrame(['No non numeric columns on dataset'],columns = ["describe_non_numeric"])
    return describe_non_numerical

if __name__ == '__main__':
    df_describe_non_num()