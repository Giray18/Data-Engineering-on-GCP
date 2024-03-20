import pandas as pd

def df_detect_xml(df: pd.DataFrame):
    ''' Returns a dataframe consisting of field names 
    that has xml format in it'''
    col_with_xml_val = [df[i].name for i in df.columns if "<" in str(df[i].iloc[0])]
    df_xml_cols = pd.DataFrame(col_with_xml_val,columns = ["col_with_xml_val"])
    return df_xml_cols


if __name__ == '__main__':
    df_detect_xml()