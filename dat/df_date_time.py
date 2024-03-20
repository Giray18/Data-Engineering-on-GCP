import pandas as pd
import re

def df_date_time(df: pd.DataFrame):
    ''' Returns a dataframe consisting of field names 
    that date time format and second dataframe shows min, max and 
    datediff between min and max'''
    #Regex pattern for date columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
    #Loop through columns detect date columns
    col_date = [df[i].name for i in df.columns if pattern_d.match(str(df[i].iloc[0])) or pattern_d_alt.match(str(df[i].iloc[0]))]
    df_date_cols = pd.DataFrame(col_date,columns = ["col_date"])
    # Getting min and max timestamps from dataset`s timestamp column
    if int(df_date_cols.size) > 0:
        min_max_date = [df[i].min() for i in df_date_cols.col_date.values]
        min_max_date.extend([df[i].max() for i in df_date_cols.col_date.values])
        range_col_date = 2*len(col_date)
        columns_min_max = [col_name for col_name in range(range_col_date)]
        df_date_min_max = pd.DataFrame([min_max_date],columns = columns_min_max)
        # Converting related columns to date-time datatype
        for i in df_date_min_max.columns:
            df_date_min_max[i] = pd.to_datetime(df_date_min_max[i], format='mixed')
        # Adding diff dates column
        counter = 0
        frames = []
        for i in range(len(col_date)):
            vars()[col_date[i]] = pd.DataFrame(df_date_min_max[counter + 1] - df_date_min_max[counter],columns = ["date_diff"])
            # vars()[col_date[i]] = df_date_min_max[counter + 1] - df_date_min_max[counter]
            counter += 2
            frames.append(vars()[col_date[i]])
        frames.extend([df_date_cols,df_date_min_max])
        # Concat all dataframes into one
        df_ultimate = pd.concat(frames)
    else:
        df_ultimate = pd.DataFrame(['No date columns on dataset'],columns = ["date_time"])
    return df_ultimate


if __name__ == '__main__':
    df_date_time()