import pandas as pd
import numpy as np
import sys


def extract_data(source):
   return pd.read_csv(source)

def transform_data(data):
    df1=data.copy()
    df1.index.duplicated(keep = 'first').sum()
    df1.drop_duplicates(inplace=True)
    #print("renaming columns")
    df1.rename(columns={'country': 'country name'}, inplace=True)
    df1.drop(columns=['draft_year'], inplace=True)
 
    return df1

def load_data(data,outcome):
    data.to_csv(outcome)


if __name__ =="__main__":
    Z= sys.argv
    print("starting")
    df=extract_data(Z[1])
    new_data=transform_data(df)
    load_data(new_data, Z[2])
    print("task completed")

   
    

