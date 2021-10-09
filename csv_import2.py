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

# %% import libraries 
import os

import numpy as np
import pandas as pd

import asyncio
import asyncpg
import nest_asyncio
nest_asyncio.apply()

# %%
#find csv files in current working directory
#isolate only the csv files
#make a new directory
#move the csv files in the new directory to process in isolation
# !ls #lists all files
csv_files = []
for file in os.listdir(os.getcwd()):
    if file.endswith('.csv') and '_' not in file:
        csv_files.append(file)
print(csv_files)

# %% create a new directory
dataset_dir = 'datasets'

#create bash command to make a new directory
#   mkdir dataset_dir
# mkdir = 'mkdir {0}'.format(dataset_dir)
try:
    mkdir = f'mkdir {dataset_dir}'
    os.system(mkdir)
except: 
    pass

#%% move the csv files in the new directory
#mv filename directory
for csv in csv_files:
    mv_file = f"mv '{csv}' {dataset_dir}"
    os.system(mv_file)

# %% create the pandas df from the csv file
data_path = os.getcwd()+'/'+dataset_dir+'/'

df = {}
for file in csv_files:
    try:
        df[file] = pd.read_csv(data_path+file)
    except UnicodeDecodeError:
        df[file] = pd.read_csv(data_path+file, encoding="ISO-8859-1")
    print(file)

# %% clean table names 
for k in csv_files:
    dataframe = df[k]
    clean_tbl_name = k.lower().replace(" ", "_")\
        .replace("-","_").replace("?","_").replace(r"/", "_")\
        .replace(")", "").replace(r"(", "").replace("%", "")\
        .replace("?", "").replace("\\", "_").replace("$","")

    #remove .csv extension from clean_tbl_name
    tbl_name = f'{clean_tbl_name}'.split('.')[0]

    dataframe.columns = [x.lower().replace(" ", "_")\
        .replace("-","_").replace("?","_").replace(r"/", "_")\
        .replace(")", "").replace(r"(", "").replace("%", "")\
        .replace("?", "").replace("\\", "_").replace("$","") for x in dataframe.columns]

    #replacement dictionary that maps pandas dtypes to SQL dtypes
    replacements = {
    'object': 'varchar',
    'float64': 'float',
    'int64' : 'int',
    'datetime64': 'timestamp',
    'timedelta64[ns]': 'varchar'
    }

    #table schema
    col_str = ", ".join("{} {}".format(n, d) for (n, d) in zip(dataframe.columns, dataframe.dtypes.replace(replacements)))

    def typeClean(str, type_str):
        #takes in the str of values from a row in the csv
        #checks converts all integers or floats into the appropriate datatype
        #and returns an array of each value as the appropriate dtype
        col_vars = type_str.split(', ')
        str_arr = str.strip().split(',')
        for i in range(len(str_arr)):
            col_type = col_vars[i].split(' ')[1]
            if col_type == 'int':
                str_arr[i] = int(str_arr[i])
            elif col_type == 'float':
                str_arr[i] = float(str_arr[i])
        return str_arr

    #cloud sql connection - CREATE TABLE AFTER DROPPING TABLES WITH SAME NAME
    async def run():
        conn = await asyncpg.connect(user=user, password=password, database=database, host=ip)
        print('connected')
        await conn.execute(f'DROP TABLE IF EXISTS {tbl_name}')
        await conn.execute(f'''
            CREATE TABLE {tbl_name} (
                {col_str}
            );
        ''')
        print(f'{tbl_name} was created successfully')

        #upload info into associates table 
        dataframe.to_csv(k, header=dataframe.columns, index= False, encoding='utf-8')
        values = []
        with open(k, 'r') as f:
            next(f)
            for row in f:
                values.append(tuple(typeClean(row, col_str)))
            
            # await conn.execute('INSERT INTO customer_contracts VALUES ($1, $2, $3, $4, $5, $6)', values)
        
        result = await conn.copy_records_to_table(
            tbl_name, records=values
        )
        print(result, f'import to {tbl_name} complete')
        await conn.close() #close the connection
    loop = asyncio.get_event_loop() #can also make single line
    loop.run_until_complete(run())

#print loop end msg
print('all tables successfully imported')

# %% USER AUTH FOR GOOGLE CLOUD DATABASE
user = "postgres"
password = "jamesiscool"
database = "postgres"
ip = "35.203.71.161"


# %%
