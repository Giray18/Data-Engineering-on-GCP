import pandas as pd

def df_shape(df: pd.DataFrame):
    ''' Returns row and column size of dataframe'''
    shape_df = df.shape
    df_shape = pd.DataFrame([shape_df],columns = ["rows","cols"])
    return df_shape

if __name__ == '__main__':
    df_shape()