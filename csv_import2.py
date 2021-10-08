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
# cloud sql connection - CREATE TABLE AFTER DROPPING TABLES WITH SAME NAME
import asyncio
import asyncpg
import nest_asyncio
nest_asyncio.apply()

async def run():
    conn = await asyncpg.connect(user="postgres", password="jamesiscool", database="postgres", host="35.203.71.161")
    print('connected')
    await conn.execute('DROP TABLE IF EXISTS customer_contracts')
    await conn.execute('''
        CREATE TABLE customer_contracts (
            customer_name varchar, 
            start_date varchar, 
            end_date varchar, 
            contract_amount_m float, 
            invoice_sent varchar, 
            paid varchar
        );
    ''')

    await conn.close() #close the connection

loop = asyncio.get_event_loop() #can also make single line
loop.run_until_complete(run())

# %%
# save data to CSV in memory
df.to_csv('customer_contracts.csv', header=df.columns, index= False, encoding='utf-8')
my_file = open('customer_contracts.csv')
print('file opened in memory')

# %%
# cloud sql connection - INSERT VALUES FROM CSV INTO TABLE
import asyncio
import asyncpg
import nest_asyncio
nest_asyncio.apply()

async def run():
    conn = await asyncpg.connect(user="postgres", password="jamesiscool", database="postgres", host="35.203.71.161")
    print('connected')
    df.to_csv('customer_contracts.csv', header=df.columns, index= False, encoding='utf-8')
    my_file = open('customer_contracts.csv')
    print(my_file)
    # result = conn.copy_records_to_table(
    #     'customer_contracts', my_file
    # )
    await conn.close() #close the connection

loop = asyncio.get_event_loop() #can also make single line
loop.run_until_complete(run())

# %%
