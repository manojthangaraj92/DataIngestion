
#Import the required libraries
import logging
import os
import subprocess
import yaml
import pandas as pd
import datetime 
import gc
import re

def read_config_file(filepath):
    '''Takes in the filepath and read the file
    Args: Filepath
    return: Yaml config'''
    with open(filepath, 'r') as stream:   #open the file as read only
        try:
            return yaml.safe_load(stream)
        except yaml.YAMLError as exc:     #output an error if file not found
            logging.error(exc)


def replacer(string, char):
    '''Takes in the string and the character and return after removing unnecessary characters
    Args: string, char
    return: string'''
    pattern = char + '{2,}'
    string = re.sub(pattern, char, string)   #using regex remove unnecessary charcters
    return string

def col_header_val(df,table_config):
    '''
    replace whitespaces in the column
    and standardized column names
    Args: dataframe, table_config from YAML
    returns: validated columns after preprocessing
    '''
    df.columns = df.columns.str.lower()       #lower case
    df.columns = df.columns.str.replace('[^\w]','_',regex=True) #remove tags and replace them with underscore
    df.columns = list(map(lambda x: x.strip('_'), list(df.columns))) #strip out underscore
    df.columns = list(map(lambda x: replacer(x,'_'), list(df.columns)))
    expected_col = list(map(lambda x: x.lower(),  table_config['columns']))
    expected_col.sort()
    df.columns =list(map(lambda x: x.lower(), list(df.columns)))
    df = df.reindex(sorted(df.columns), axis=1)
    if len(df.columns) == len(expected_col) and list(expected_col)  == list(df.columns):
        print("column name and column length validation passed")
        return 1
    else:
        print("column name and column length validation failed")
        mismatched_columns_file = list(set(df.columns).difference(expected_col))
        print("Following File columns are not in the YAML file",mismatched_columns_file)
        missing_YAML_file = list(set(expected_col).difference(df.columns))
        print("Following YAML columns are not in the file uploaded",missing_YAML_file)
        logging.info(f'df columns: {df.columns}')
        logging.info(f'expected columns: {expected_col}')
        return 0
