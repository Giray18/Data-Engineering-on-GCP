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
import os


def find_value_sqlite(database_name:str, value:str):
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
    global frames
    frames = []
    
    for f in read_table_names:
        # read the sql tables by condition of date-time column hold
        df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1', con)
        # Getting list of date-time columns from detection dfs
        col_date = [df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))]
        if len(col_date) > 0:
            df = pd.read_sql_query(f"SELECT * FROM {f} WHERE {f}.{col_date[0]} == '{date.today()}'", con)
        else:
            df = pd.read_sql(f'SELECT * FROM {f}', con)
        df_check = pd.DataFrame([value], columns=["value"])
        for k,v in df.items():
            for k1,v1 in df_check.items():
                if v1.isin(v).any():
                    d = {"column" : [k], "table" : [f], "value" : [value]}
                    df_output = pd.DataFrame(data=d)
                    frames.append(df_output)
                    df_ultimate = pd.concat(frames)
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
     # Saving containing cols dataframe 
    writer = pd.ExcelWriter(f"find_value_{value}_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="containing_cols")
    writer.close()
    return "value find table saved"




def find_value_mysql(host, user, password, database, value:str):
    #Regex pattern for date columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
    # use glob to get all sql tables
    # in the folder
    # db=mysql.connector.connect(host="your host", user="your username", password="your_password",database="database_name")
    db=mysql.connector.connect(host, user, password, database)
    cursor=db.cursor()
    cursor.execute("SHOW TABLES")
    # saving all tables of database into a list
    read_table_names = [table_name[0] for table_name in cursor]
    global frames
    frames = []
    
    for f in read_table_names:
        # read the sql tables by condition of date-time column hold
        df_detect = pd.read_sql(f'SELECT * FROM {f} LIMIT 1', con)
        # Getting list of date-time columns from detection dfs
        col_date = [df_detect[i].name for i in df_detect.columns if pattern_d.match(str(df_detect[i].iloc[0])) or pattern_d_alt.match(str(df_detect[i].iloc[0]))]
        if len(col_date) > 0:
            df = pd.read_sql_query(f"SELECT * FROM {f} WHERE {f}.{col_date[0]} == '{date.today()}'", con)
        else:
            df = pd.read_sql(f'SELECT * FROM {f}', con)
        df_check = pd.DataFrame([value], columns=["value"])
        for k,v in df.items():
            for k1,v1 in df_check.items():
                if v1.isin(v).any():
                    d = {"column" : [k], "table" : [f], "value" : [value]}
                    df_output = pd.DataFrame(data=d)
                    frames.append(df_output)
                    df_ultimate = pd.concat(frames)
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", f'{date.today()}')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    # Saving containing cols dataframe 
    writer = pd.ExcelWriter(f"find_value_{value}_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="containing_cols")
    writer.close()
    return "value find table saved"