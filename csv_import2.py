#%%
# shift+enter to run cell
#### STEPS
# import csv file into a pandas df
# clean the table name and remove any extra spaces, symbols,
# capital letters (anything a database can't handle)
# clean the column headers and remove any extra spaces, symbols,
# capital letters (anything a database can't handle)
# write the create SQL statement 
# pass to database to create a table
# import data into the db

# %%
#import libraries 
import os

import numpy as np
import pandas as pd
import pyscopg2  # wrapper to make connection to postgreSQL database

# %%
!ls #lists all files

# %%
df = pd.read_csv("Customer Contracts$.csv")
df.head()

# %%
#clean table names 
#   lower case letters
#   remove all $
#   replace , , -, /, \, ? with _
file = "Customer Contracts$"

clean_tbl_name = file.lower().replace(" ", "_")\
    .replace("-","_").replace("?","_").replace(r"/", "_")\
    .replace(")", "").replace(r"(", "").replace("%", "")\
    .replace("?", "").replace("\\", "_").replace("$","")
clean_tbl_name
# %%
#clean header names with same rules
df.columns = [x.lower().replace(" ", "_")\
    .replace("-","_").replace("?","_").replace(r"/", "_")\
    .replace(")", "").replace(r"(", "").replace("%", "")\
    .replace("?", "").replace("\\", "_").replace("$","") for x in df.columns]
df.columns

# %%
#create a dictionary 
replacements = {
    'object': 'varchar',
    'float64': 'float',
    'int64' : 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
    }

# %%
col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(df.columns, df.dtypes.replace(replacements)))
col_str
# %%
#open a database connection
conn_str = 
# %%
import sys
sys.version
sys.path
# %%
