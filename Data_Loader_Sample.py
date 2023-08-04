import snowflake.connector as snow
import pandas as pd
from snowflake.connector.pandas_tools import write_pandas
from connection_builder import connection_builder

#connection_builder(warehouse, first_time_connection = True, database = "RAW", schema = "GLOBALMART"):

if __name__ == "__main__" :
    conn, cursor = connection_builder("COMPUTE_WH", True)

    #create a new warehouse
    warehouse_name = 'Transform'
    cursor.execute(f'CREATE WAREHOUSE IF NOT EXISTS {warehouse_name}')
    print(f'Warehouse {warehouse_name} created!\n')

    # Switch to the new warehouse
    cursor.execute(f'USE WAREHOUSE {warehouse_name}')
    print(f'Warehouse {warehouse_name} in use now.\n')

    # Create a new database for the models
    database_name1 = 'analytics'
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name1}')
    print(f'Database {database_name1} created.\n')

    # Create a new database for the raw data
    database_name2 = 'raw'
    cursor.execute(f'CREATE DATABASE IF NOT EXISTS {database_name2}')
    print(f'Database {database_name2} created.\n')

    #create a new Schema "Globalmart" under the Raw Database - here we are going to load over raw data under three tables
    schema_name = 'globalmart'
    cursor.execute(f'Create SCHEMA IF NOT EXISTS {database_name2}.{schema_name}')
    print(f'Schema {schema_name} created under {database_name2}.\n')

    conn, cursor = connection_builder("Transform", False)
    
    #read the orders table into a pandas dataframe
    orders = pd.read_csv('./Data/orders.csv')
    write_pandas(conn, orders, "ORDERS",auto_create_table=True)
    print('Orders data loaded \n')

    customers = pd.read_csv('./Data/customers.csv')
    write_pandas(conn, customers, "CUSTOMERS",auto_create_table=True)
    print('Customers data Loaded\n')

    products = pd.read_csv('./Data/product.csv')
    write_pandas(conn, products, "PRODUCTs",auto_create_table=True)
    print('Products Data loaded!')




