import snowflake.connector as snow
def connection_builder(warehouse, first_time_connection = True, database = "RAW", schema = "GLOBALMART"):
    username = "vishnurajalingam"
    password = "Sanguine@93"
    account = "fq92599.canada-central.azure"

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