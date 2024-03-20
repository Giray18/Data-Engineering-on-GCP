import pandas as pd

def df_duplicate_count(df: pd.DataFrame):
    ''' Returns duplicate rows count/qty on dataset'''
    col_with_list = [df[i].name for i in df.columns if "[" in str(df[i].iloc[0]) or "{" in str(df[i].iloc[0])]
    for k,v in df.items():
        if k in col_with_list:
            df[k] = [','.join(map(str, l)) for l in df[k]]
    duplicate_numb = len(df)-len(df.drop_duplicates())
    df_duplicate = pd.DataFrame([duplicate_numb],columns = ["duplicate_qty"])
    return df_duplicate

if __name__ == '__main__':
    df_duplicate_count()