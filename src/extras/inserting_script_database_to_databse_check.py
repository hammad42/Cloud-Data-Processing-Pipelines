
from sqlalchemy import create_engine
import sqlalchemy.exc
import pandas as pd 
import os

timestamp = pd.Timestamp.now()

database_username = os.environ.get("database_username_local")
database_password = os.environ.get("database_password_local")
database_name = os.environ.get("database_name_local")
database_host = os.environ.get("database_host_local") # Find this in your instance details

database_username_azure = os.environ.get("database_username_azure")
database_password_azure = os.environ.get("database_password_azure")
database_name_azure = os.environ.get("database_name_azure")
database_host_azure = os.environ.get("database_host_azure") # Find this in your instance details transactionaldataserver.database.windows.net
x=0
print(database_host_azure)

database_connection_string = 'mysql+pymysql://{}:{}@{}/{}'.format(
    database_username, database_password, database_host, database_name
)

try:
    engine = create_engine(database_connection_string)
    with engine.connect() as connection: 
        connection.execute("SELECT 1")  # Simple test query 
    print("Database connection successful with local!")
except sqlalchemy.exc.OperationalError as e:
    print(f"Connection failed with local: {e}")

engine_url = f'mssql+pyodbc://{database_username_azure}:{database_password_azure}@{database_host_azure}/{database_name_azure}?driver=ODBC+Driver+18+for+SQL+Server'

# Create the engine
engine = create_engine(engine_url)
try:
    with engine.connect() as connection:
        result = connection.execute("SELECT 1")
        print("Database connection successful with azure!")
        # row = result.fetchone()
        # print(row)
except sqlalchemy.exc.OperationalError as e:
    print(f"Connection failed with azure: {e}")
