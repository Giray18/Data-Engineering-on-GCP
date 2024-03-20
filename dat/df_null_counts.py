import pandas as pd

def df_null_counts(df: pd.DataFrame):
    ''' Returns null value count and percent of null counts on columns'''
    null_val_holding_cols = [df[i].isna().sum() for i in df.columns]
    null_val_percent = [df[i].isna().sum()/df[i].count() for i in df.columns]
    null_val_percent = ['{:.1%}'.format(member) for member in null_val_percent]
    df_null = pd.DataFrame([null_val_holding_cols],columns = df.columns)
    df_null_percent = pd.DataFrame([null_val_percent],columns = df.columns)
    union_null = pd.concat([df_null,df_null_percent])
    return union_null

if __name__ == '__main__':
    df_null_counts()