import snowflake.connector as snow
def connection_builder(first_time_connection = True, schema = "Analtyics"):
    username = "vishnurajalingam"
    password = "Sanguine@93"
    database = "Store"
    account = "fq92599.canada-central.azure"
    warehouse = "COMPUTE_WH"

    if(first_time_connection):
        conn = snow.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse
        )
        cursor = conn.cursor()
        return conn, cursor
    else:
        conn = snow.connect(
            user=username,
            password=password,
            account=account,
            warehouse=warehouse,
            database = database,
            schema = schema
        )
        cursor = conn.cursor()
        return conn, cursor