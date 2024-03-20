import pandas as pd
import dat

def analysis_dict():
    ''' Returns a dict consisting of dataframe names and dat package functions'''
    analysis_dict = {"df": dat.df_flatten_json,"head": dat.df_head , 
                     "col_types" : dat.df_col_types, "shape" :dat.df_shape,
                     "describe_non_num" : dat.df_describe_non_num, "duplicate_count" : dat.df_duplicate_count,
                     "unique_cols" : dat.df_unique_cols, "null_counts" : dat.df_null_counts,
                     "json_cols" : dat.df_detect_json, "xml_cols" : dat.df_detect_xml,
                     "date_cols" :dat.df_date_time, "value_counts" : dat.df_value_counts}

    return analysis_dict

if __name__ == '__main__':
    analysis_dict()