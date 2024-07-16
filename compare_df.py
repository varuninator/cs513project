import pandas as pd
import numpy as np

def compare_dataframes(original_df, updated_df):
    if original_df.shape != updated_df.shape:
        raise ValueError("The input dataframes must have the same shape.")
    
    diff_df = pd.DataFrame(np.nan, index=original_df.index, columns=original_df.columns)
    
    for column in original_df.columns:
        diff_mask = original_df[column] != updated_df[column]
        diff_df.loc[diff_mask, column] = updated_df.loc[diff_mask, column]

    diff_df = diff_df.dropna(how='all')

    return diff_df