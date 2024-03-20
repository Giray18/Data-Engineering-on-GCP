import pandas as pd

def df_unique_cols(df: pd.DataFrame):
    ''' Returns name of unique value holding columns on dataset'''
    unique_val_holding_cols = [i for i in df.columns if len(df[i]) == len(pd.unique(df[i]))]
    df_unique = pd.DataFrame(unique_val_holding_cols,columns = ["unique_val_holding_cols"])
    return df_unique

if __name__ == '__main__':
    df_unique_cols()