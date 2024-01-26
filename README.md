import requests
import pandas as pd
file_path = r'C:\Users\KMhlon438\Downloads\PAYMENT_PROFILE_UPLOAD7.csv'
df = pd.read_csv(file_path, delimiter='|')

# Specify the path for the output CSV file
output_path =  r'C:\Users\KMhlon438\Downloads\output_file.csv' 

# Save the DataFrame as a CSV file
df.to_csv(output_path, index=False)

print("Conversion complete!")

df.head(5)


# from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
import pandas as pd
import numpy as np

conn = snowflake.connector.connect(user='SVC_DATA_SCIENCE',
       password='v88cbx2ZqS5ZTSBM5HvE#@@!',
       account='pm58521.east-us-2.azure')
# Create a connection to the database
cur = conn.cursor()

sql = 'USE DATABASE DATAWAREHOUSE'
cur.execute(sql)

sql = 'USE SCHEMA DATA_SCIENCE_USER'
cur.execute(sql)

df = pd.read_csv('output_file.csv')

<snowflake.connector.cursor.SnowflakeCursor at 0x1bbbb8607f0>
<snowflake.connector.cursor.SnowflakeCursor at 0x1bbbb8607f0>
C:\Users\KMhlon438\AppData\Local\Temp\ipykernel_10596\3866723669.py:1: DtypeWarning: Columns (2) have mixed types. Specify dtype option on import or set low_memory=False.
  df = pd.read_csv('output_file.csv')


  def get_col_types(df):
    
    '''
        Helper function to create/modify Snowflake tables; gets the column and dtype pair for each item in the dataframe

        
        args:
            df: dataframe to evaluate
            
    '''
    
    # get dtypes and convert to df
    ct = df.dtypes.reset_index().rename(columns={0:'col'})
    ct = ct.apply(lambda x: x.astype(str).str.upper()) # case matching as snowflake needs it in uppers
        
    # only considers objects at this point
    # only considers objects and ints at this point
    ct['col'] = np.where(ct['col']=='OBJECT', 'VARCHAR', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('DATE'), 'DATETIME', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('INT'), 'NUMERIC', ct['col'])
    ct['col'] = np.where(ct['col'].str.contains('FLOAT'), 'FLOAT', ct['col'])
    
    # get the column dtype pair
    l = []
    for index, row in ct.iterrows():
        l.append(row['index'] + ' ' + row['col'])
    
    string = ', '.join(l) # convert from list to a string object
    
    string = string.strip()
    
    return string


    def create_table(table, action, col_type, df):
    
    '''
        Function to create/replace and append to tables in Snowflake
        
        args:
            table: name of the table to create/modify
            action: whether do the initial create/replace or appending; key to control logic
            col_type: string with column name associated dtype, each pair separated by a comma; comes from get_col_types() func
            df: dataframe to load
            
        dependencies: function get_col_types(); helper function to get the col and dtypes to create a table
    '''
    
    
    if action=='create_replace':
    
        # set up execute
        cur.execute('CREATE OR REPLACE TABLE '+table+'('+col_type+')')

        #prep to ensure proper case
        df.columns = [col.upper() for col in df.columns]

        # write df to table
        write_pandas(conn, df, table.upper())
        
    elif action=='append':
        
        # convert to a string list of tuples
        df = str(list(df.itertuples(index=False, name=None)))
        # get rid of the list elements so it is a string tuple list
        df = df.replace('[','').replace(']','')
        
        # set up execute
        cur.execute('INSERT INTO '+table+' VALUES '+df)


        df = df.astype(str)

        col_type = get_col_types(df)
create_table('PAYMENT_', 'create_replace', col_type, df)

cur.close()


