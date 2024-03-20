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


def data_dict_crate_sqlite(database_name:str):
    ''' This function reads multiple tables from connected database and creates
    data dictionary of all columns exists on tables '''
    #Regex pattern for columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
    pattern_a_numeric = re.compile(r"[A-Za-z0-9]+", re.IGNORECASE)
    pattern_text = re.compile(r"[A-Za-z]+", re.IGNORECASE)
    pattern_float = re.compile(r"[0-9]*\.[0-9]+", re.IGNORECASE)
    pattern_int = re.compile(r"[0-9]+", re.IGNORECASE)
    pattern_phone = re.compile(r"\+[0-9]+.*", re.IGNORECASE)
    pattern_email = re.compile(r"[A-Za-z0-9]+@", re.IGNORECASE)

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
        col_a_num = [df_detect[i].name for i in df_detect.columns if pattern_a_numeric.match(str(df_detect[i].iloc[0]))]
        col_text = [df_detect[i].name for i in df_detect.columns if pattern_text.match(str(df_detect[i].iloc[0]))]
        col_float = [df_detect[i].name for i in df_detect.columns if pattern_float.match(str(df_detect[i].iloc[0]))]
        col_int = [df_detect[i].name for i in df_detect.columns if pattern_int.match(str(df_detect[i].iloc[0]))]
        col_phone = [df_detect[i].name for i in df_detect.columns if pattern_phone.match(str(df_detect[i].iloc[0]))]
        col_email = [df_detect[i].name for i in df_detect.columns if pattern_email.match(str(df_detect[i].iloc[0]))]
        for k,v in df_detect.items():
            if k in col_date:
                d = {"column" : [k], "table" : [f], "dtype" : "date"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_phone:
                d = {"column" : [k], "table" : [f], "dtype" : "phone_num"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_email:
                d = {"column" : [k], "table" : [f], "dtype" : "email"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_text:
                d = {"column" : [k], "table" : [f], "dtype" : "text"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_float:
                d = {"column" : [k], "table" : [f], "dtype" : "float"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_int:
                d = {"column" : [k], "table" : [f], "dtype" : "int"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_a_num:
                d = {"column" : [k], "table" : [f], "dtype" : "a_numeric"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
        df_ultimate = pd.concat(frames)
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", 'data_dict')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    # Saving containing cols dataframe 
    writer = pd.ExcelWriter(f"data_dict_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="data_dict")
    writer.close()
    return "data dict saved"



def data_dict_crate_mysql(database_name:str):
    ''' This function reads multiple tables from connected database and creates
    data dictionary of all columns exists on tables '''
    #Regex pattern for columns
    pattern_d = re.compile(r"[0-9]{4}.[0-9]{2}.[0-9]{2}.*", re.IGNORECASE)
    pattern_d_alt = re.compile(r"[0-9]{2}.[0-9]{2}.[0-9]{4}.*", re.IGNORECASE)
    pattern_a_numeric = re.compile(r"[A-Za-z0-9]+", re.IGNORECASE)
    pattern_text = re.compile(r"[A-Za-z]+", re.IGNORECASE)
    pattern_float = re.compile(r"[0-9]*\.[0-9]+", re.IGNORECASE)
    pattern_int = re.compile(r"[0-9]+", re.IGNORECASE)
    pattern_phone = re.compile(r"\+[0-9]+.*", re.IGNORECASE)
    pattern_email = re.compile(r"[A-Za-z0-9]+@", re.IGNORECASE)

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
        col_a_num = [df_detect[i].name for i in df_detect.columns if pattern_a_numeric.match(str(df_detect[i].iloc[0]))]
        col_text = [df_detect[i].name for i in df_detect.columns if pattern_text.match(str(df_detect[i].iloc[0]))]
        col_float = [df_detect[i].name for i in df_detect.columns if pattern_float.match(str(df_detect[i].iloc[0]))]
        col_int = [df_detect[i].name for i in df_detect.columns if pattern_int.match(str(df_detect[i].iloc[0]))]
        col_phone = [df_detect[i].name for i in df_detect.columns if pattern_phone.match(str(df_detect[i].iloc[0]))]
        col_email = [df_detect[i].name for i in df_detect.columns if pattern_email.match(str(df_detect[i].iloc[0]))]
        for k,v in df_detect.items():
            if k in col_date:
                d = {"column" : [k], "table" : [f], "dtype" : "date"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_phone:
                d = {"column" : [k], "table" : [f], "dtype" : "phone_num"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_email:
                d = {"column" : [k], "table" : [f], "dtype" : "email"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_text:
                d = {"column" : [k], "table" : [f], "dtype" : "text"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_float:
                d = {"column" : [k], "table" : [f], "dtype" : "float"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_int:
                d = {"column" : [k], "table" : [f], "dtype" : "int"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
            elif k in col_a_num:
                d = {"column" : [k], "table" : [f], "dtype" : "a_numeric"}
                df_output = pd.DataFrame(data=d)
                frames.append(df_output)
        df_ultimate = pd.concat(frames)
    dir = os.path.join("C:\\", "Users\MeliaMuyo\Desktop\ALL\GIRAY\data_analysis_tool", 'data_dict')
    if not os.path.exists(dir):
        os.mkdir(dir)
    os.chdir(dir)
    # Saving containing cols dataframe 
    writer = pd.ExcelWriter(f"data_dict_{date.today()}.xlsx", engine="xlsxwriter")
    df_ultimate.to_excel(writer,sheet_name="data_dict")
    writer.close()
    return "data dict saved"