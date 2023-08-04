import snowflake.connector as snow
import pandas as pd
def write_dataframe_to_snowflake(df, full_table_name, username = "vishnurajalingam", password = "Sanguine@93", account = "fq92599.canada-central.azure", warehouse = "Transform"):
    # Split the full_table_name into database, schema, and table_name
    database, schema, table_name = full_table_name.split('.')

    # Create a connection to Snowflake
    conn = snow.connect(
        user=username,
        password=password,
        account=account,
        warehouse=warehouse,
        database=database,
    )

    cursor = conn.cursor()

    df['ORDERSELLINGPRICE'] = df['ORDERSELLINGPRICE'].astype('int64')

    # Convert the DataFrame to a Pandas DataFrame and upload it to Snowflake
    pd.DataFrame.to_sql(df, name=table_name, con=conn, schema=schema, index=False, if_exists='append')

    # Close the connection
    cursor.close()
    conn.close()