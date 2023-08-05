from connection_builder import connection_builder

def create_role_and_user():
    conn, cursor = connection_builder(True)
    cursor.execute('USE ROLE ACCOUNTADMIN')
    cursor.execute('CREATE ROLE IF NOT EXISTS transform')
    print('Role \'Transform\' Created\n')
    cursor.execute('GRANT ROLE TRANSFORM TO ROLE ACCOUNTADMIN')
    print('Role \'Transform\' now has Account Admin level privileges\n')
    create_user = '''
    CREATE USER IF NOT EXISTS modeler
    PASSWORD='Sanguine@93'
    LOGIN_NAME='dbt'
    MUST_CHANGE_PASSWORD=FALSE
    DEFAULT_WAREHOUSE='COMPUTE_WH'
    DEFAULT_ROLE='transform'
    DEFAULT_NAMESPACE='Store.RAW'
    COMMENT='DBT user used for data transformation'
    '''

    cursor.execute(create_user)
    print('User \'Modeler\' with a login name of \'dbt\' has been created and is given the \'Transform\' role. \n')

    cursor.execute('GRANT ROLE transform to USER modeler')
    cursor.execute('GRANT ALL ON WAREHOUSE COMPUTE_WH TO ROLE transform')
    cursor.execute('GRANT ALL ON DATABASE Store to ROLE transform')
    cursor.execute('GRANT ALL ON ALL SCHEMAS IN DATABASE Store to ROLE transform')
    cursor.execute('GRANT ALL ON FUTURE SCHEMAS IN DATABASE Store to ROLE transform')
    cursor.execute('GRANT ALL ON ALL TABLES IN SCHEMA Store.RAW to ROLE transform')
    cursor.execute('GRANT ALL ON FUTURE TABLES IN SCHEMA Store.RAW to ROLE transform')
    print('Highest priviledges given to the user \'Modeler\'')
    conn.close()