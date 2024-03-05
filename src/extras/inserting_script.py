
from sqlalchemy import create_engine
import sqlalchemy.exc
import pandas as pd 
timestamp = pd.Timestamp.now()

database_username = "root"
database_password = "Karachi.321"
database_name = "planes"
database_host = '34.31.88.172' # Find this in your instance details
x=0

database_connection_string = 'mysql+pymysql://{}:{}@{}/{}'.format(
    database_username, database_password, database_host, database_name
)

try:
    engine = create_engine(database_connection_string)
    with engine.connect() as connection: 
        connection.execute("SELECT 1")  # Simple test query 
    print("Database connection successful!")
except sqlalchemy.exc.OperationalError as e:
    print(f"Connection failed: {e}")
df=pd.read_csv("planes.csv",skiprows=x,nrows=500,header=None,names=['tailnum', 'year', 'type','manufacturer', 'model', 'engines','seats','speed','engine'])
increasing_numbers = pd.Series(range(x, x+len(df) ))
df['pri_key']=increasing_numbers
print(df.head())
result = df.to_sql(name='planes', con=engine, if_exists='append', index=False) 
if result > 0:
    print(f"{result} rows inserted successfully!")
else:
    print("No rows were inserted.")
