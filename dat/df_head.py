import pandas as pd

def df_head(df: pd.DataFrame,line_number = 5):
    ''' Returns first lines of a dataframe
    for quick look of table field values'''
    df_head = df.head(line_number)
    return df_head

if __name__ == '__main__':
    df_head()
