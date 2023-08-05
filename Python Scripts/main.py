import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from user_and_role_setup import create_role_and_user
from connection_builder import connection_builder

def data_loader(table_name, location):
    df = pd.read_csv(location)
    conn, cursor = connection_builder(False, "RAW")
    write_pandas(conn, df, table_name,auto_create_table=True)
    print(f'{table_name} data loaded \n')
    conn.close()

if __name__ == "__main__" :
    conn, cursor = connection_builder(True)
    
    # Create a new database for the models
    database_name = 'Store'
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name}')
    database_name = 'Store'
    print(f'Database \"{database_name}\" created.\n')

    #create a Schema "Analytics" - This is where are the views and models from Raw will be populated
    schema1 = 'Analytics'
    cursor.execute(f'Create SCHEMA IF NOT EXISTS {database_name}.{schema1}')
    print(f'Schema \'{schema1}\' created under {database_name}.\n')

    #create a Schema "Raw" - This is where the Raw data will be stored
    schema2 = 'Raw'
    cursor.execute(f'Create SCHEMA IF NOT EXISTS {database_name}.{schema2}')
    print(f'Schema \'{schema2}\' created under {database_name}.\n')
    
    #now to load the data onto Tables in Snowflake
    table_names = ['ORDERS', "CUSTOMERS", "PRODUCTS"]
    locations = ['../Data/orders.csv', '../Data/customers.csv', '../Data/product.csv']

    for table_name, location in zip(table_names, locations):
        data_loader(table_name, location)
    
    #now to create a user and assign him priviledges - so as to be able for use in DBT Transformations
    create_role_and_user()