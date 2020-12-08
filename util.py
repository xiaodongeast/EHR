import pandas as pd
import numpy as np

import matplotlib.pyplot as plt
import seaborn as sns

import datetime
# set display


def min_max_diff(df_encounter,list_of_interest):
    """I will use this to find the the min, max, mean, and the maximium change """
    df = df_encounter.copy()
    for col in list_of_interest:
        df['min'+col] = df[col].apply(np.min)
        df['max'+col]= df[col].apply(np.max)
        df['mean'+col] = df[col].apply(np.mean)
        df['median'+col] = df[col].apply(np.median)
        df['diff'+col] = df['max'+col] - df['min'+col]
    return df


def create_encounter(df, grouping_field_list = ['GRID']):
    """change data to the encounter """
    non_grouped_field_list = [c for c in df.columns if c not in grouping_field_list]
    encounter_df = df.groupby(grouping_field_list)[non_grouped_field_list].\
        agg(lambda x: list([y for y in x if y is not np.nan])).reset_index()
    return encounter_df


def find_m(time_series, values_s):
    '''
    if the list is empty return -1
    else median
    '''
    if sum(time_series) == 0:  # zero
        return -1
    else:
        value = (values_s[time_series]).values.flatten()
        # print(value, np.median(value))
        return np.median(value)


def first_occurance(dates, values, cutoff=6.5):
    """
    the values need to be sorted with time.
    """
    if values[0] >= cutoff:
        return None
    else:
        return dates[np.argmax(values >= cutoff)]  


def intersect_df(df1, df2, primary_key ='GRID'):
    df1_selected = df1[df1[primary_key].isin(df2[primary_key])]
    df2_selected = df2[df2[primary_key].isin(df1[primary_key])]
    return  df1_selected, df2_selected
