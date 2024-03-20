import pandas as pd
import os
import glob
import numpy as np
import dat
from datetime import datetime, timedelta, date
import sqlite3
import mysql
import xlsxwriter
import openpyxl
import re

def multiple_dataset_apply_mysql(host:str, user:str, password:str, database:str):
    ''' This function reads multiple tables from connected database as parameter 
    then runs all functions on dat package to read table '''
    #Regex pattern for date columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)

    # db=mysql.connector.connect(host="your host", user="your username", password="your_password",database="database_name")
    db=mysql.connector.connect(host, user, password, database)
    cursor=db.cursor()
    cursor.execute("SHOW TABLES")
    # saving all tables of database into a list
    read_table_names = [table_name[0] for table_name in cursor]
    # Dict for dataframes
    dataframes_dict = {}

    # Creating working directory for daily partitioning
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)

    # loop over the list of sql tables
    for f in read_table_names:
        print(f)
        # read the sql tables by condition of date-time column hold
        df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1', cursor)
        # Getting list of date-time columns from detection dfs
        col_date = [df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))]
        if len(col_date) > 0:
            df = pd.read_sql_query(f"SELECT * FROM {f} WHERE {f}.{col_date[0]} == '{date.today()}'", cursor)
        else:
            df = pd.read_sql(f'SELECT * FROM {f}', cursor)
        # Creating a dict consisting of all dataframes
        if df.size != 0:
            dataframes_dict[f] = df  
        # Creating dataframes defined on analysis_dict.py file
        for key,value in dat.analysis_dict().items():
            if df.size != 0:
                vars()[key] = value(df)
                dat.save_dataframe_excel(vars(),f"analysis_{f}_{date.today()}")
            else:
                dat.save_dataframe_excel(df,f"analysis_{f}_{date.today()}")

    # loop over the list of sql tables
    #for f in read_table_names:
    #    print(f)
        # read the sql tables
        #df = pd.read_csv(f, low_memory=False)
    #    df = pd.read_sql(f'SELECT * FROM {f}', db)
        # Creating a dict consisting of all dataframes
    #    dataframes_dict[f] = df
        # Creating dataframes defined on analysis_dict.py file
    #    for key,value in dat.analysis_dict().items():
    #        vars()[key] = value(df)
            # Saving dataframes consisting of analysis into a single excel file
    #        dat.save_dataframe_excel(vars(),f"analysis_{f}_{date.today()}")
    return "dataset_analysis_saved"

def multiple_dataset_apply_sqlite(database_name:str):
    ''' This function reads multiple tables from connected database as parameter 
    then runs all functions on dat package to read table '''
    #Regex pattern for date columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
    
    # use glob to get all sql tables
    # in the folder
    con = sqlite3.connect(database_name)
    # con.text_factory = lambda x: str(x, encoding)
    con.text_factory = lambda b: b.decode(errors = 'ignore')
    cur = con.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type = 'table'")
    # saving all tables of database into a list
    read_table_names = [table_name[0] for table_name in cur]
    # Dict for dataframes
    dataframes_dict = {}

    # Creating working directory for daily partitioning
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)


    # loop over the list of sql tables
    for f in read_table_names:
        print(f)
        # read the sql tables by condition of date-time column hold
        df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1', con)
        # Getting list of date-time columns from detection dfs
        col_date = [df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))]
        if len(col_date) > 0:
            df = pd.read_sql_query(f"SELECT * FROM {f} WHERE {f}.{col_date[0]} == '{date.today()}'", con)
        else:
            df = pd.read_sql(f'SELECT * FROM {f}', con)
        # Creating a dict consisting of all dataframes
        if df.size != 0:
            dataframes_dict[f] = df  
        # Creating dataframes defined on analysis_dict.py file
        for key,value in dat.analysis_dict().items():
            if df.size != 0:
                vars()[key] = value(df)
                dat.save_dataframe_excel(vars(),f"analysis_{f}_{date.today()}")
            else:
                dat.save_dataframe_excel(df,f"analysis_{f}_{date.today()}")
            # Saving dataframes consisting of analysis into a single excel file
        # alldfs = [var for var in dir() if isinstance(eval(var), pd.core.frame.DataFrame)]
    return dataframes_dict

def multiple_dataset_apply_containing_cols_sqlite(database_name = 'str'):
    # containing column detection run starts from here
    global frames
    frames = []
    global df_ultimate
    df_ultimate = pd.DataFrame()
    # Running dataframe save function and apply normal transformations first
    dataframes_dict = multiple_dataset_apply_sqlite(database_name)
    for key,value in dataframes_dict.items():
        for key_col,columnData in value.items():
            for key_1,value_1 in dataframes_dict.items():
                for key_col_1,colondata in value_1.items():
                    if columnData.isin(colondata).all() == True and key_col != key_col_1 and "Id" not in key_col and "Id" not in key_col_1:
                        d = {"table_1" : [key], "col_1" : [key_col], "table_2" : [key_1], "col_2" : [key_col_1]}
                        df_output = pd.DataFrame(data=d)
                        frames.append(df_output)
                        #global df_ultimate
                        df_ultimate = pd.concat(frames)

    # Creating working directory for daily partitioning
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    # Saving containing cols dataframe 
    writer = pd.ExcelWriter(f"containing_cols_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="containing_cols")
    writer.close()
    return "output files saved"

def multiple_dataset_apply_containing_cols_mysql(host:str, user:str, password:str, database:str):
    # containing column detection run starts from here
    global frames
    frames = []
    dataframes_dict = multiple_dataset_apply_mysql(host, user, password, database)
    for key,value in dataframes_dict.items():
        for key_col,columnData in value.items():
            for key_1,value_1 in dataframes_dict.items():
                for key_col_1,colondata in value_1.items():
                    if columnData.isin(colondata).all() == True and key_col != key_col_1 and "Id" not in key_col and "Id" not in key_col_1:
                        d = {"table_1" : [key], "col_1" : [key_col], "table_2" : [key_1], "col_2" : [key_col_1]}
                        df_output = pd.DataFrame(data=d)
                        frames.append(df_output)
                        global df_ultimate
                        df_ultimate = pd.concat(frames)

    # Creating working directory for daily partitioning
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    writer = pd.ExcelWriter(f"containing_cols_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="containing_cols")
    writer.close()
    return "output files saved"

