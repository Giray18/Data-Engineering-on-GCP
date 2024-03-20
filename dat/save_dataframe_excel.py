import pandas as pd
import xlsxwriter
import openpyxl

def save_dataframe_excel(df_list = {},file_name = "data_analysis"):
    ''' Saves all active dataframes into an excel sheet 
    created in session'''
    global writer
    writer = pd.ExcelWriter(f"{file_name}.xlsx", engine="xlsxwriter")
    for i in df_list:
        if type(df_list[i]) == pd.DataFrame and i != "df":
            # print(i)
            df_list[i].to_excel(writer, sheet_name = i)
    writer.close() 
    return "saved to excel"
    


if __name__ == '__main__':
    save_dataframe_excel()