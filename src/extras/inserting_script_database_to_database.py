def insertion(table):  
    from sqlalchemy import create_engine
    import sqlalchemy.exc
    import pandas as pd 
    import os

    database_username = os.environ.get("database_username_local")
    database_password = os.environ.get("database_password_local")
    database_name = os.environ.get("database_name_local")
    database_host = os.environ.get("database_host_local") # Find this in your instance details

    database_username_azure = os.environ.get("database_username_azure")
    database_password_azure = os.environ.get("database_password_azure")
    database_name_azure = os.environ.get("database_name_azure")
    database_host_azure = os.environ.get("database_host_azure") # Find this in your instance details transactionaldataserver.database.windows.net
    # table="titles"

    # **Database 1 (MySQL - Source)**
    db1_connection_string = 'mysql+pymysql://{}:{}@{}/{}'.format(
        database_username, database_password, database_host, database_name
    )
    db1_engine = create_engine(db1_connection_string)

    # **Database 2 (Azure SQL Server - Destination)**
    db2_engine_url = f'mssql+pyodbc://{database_username_azure}:{database_password_azure}@{database_host_azure}/{database_name_azure}?driver=ODBC+Driver+18+for+SQL+Server'
    db2_engine = create_engine(db2_engine_url)

    # **1. Query Data from Database 1**
    query = """
    SELECT *
    FROM {}
    where emp_no >= 11550 and emp_no < 12000
    """.format(table)
    print(query)
    try:
        df = pd.read_sql(query, db1_engine)
        print("Data retrieved from the first database.")
    except sqlalchemy.exc.OperationalError as e:
        print(f"Query from the first database failed: {e}")

    # **2. Load into the Azure Database**
    try:
        result=df.to_sql(table, db2_engine, if_exists='append', index=False)  
        inserted_rows = result
        print(f"Data loaded into the Azure database. Inserted rows: {inserted_rows}")
    except sqlalchemy.exc.OperationalError as e:
        print(f"Loading into the Azure database failed: {e}")

tables=("employees","dept_emp","dept_manager","salaries","titles")
for table in tables:
    insertion(table)
