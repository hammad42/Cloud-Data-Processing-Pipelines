import pandas as pd

chunksize = 1000  # Adjust as needed
x=10
chunk=pd.read_parquet("FactStoreBudget_sample.snappy.parquet")
chunk["BusinessDate"]=chunk["BusinessDate"].astype(object)
chunk["sysDateModified"]=chunk["sysDateModified"].astype(object)

# Skip the first 10 rows in each chunk
for x in range(0,201,10):
    y=x+10

    chunk_ = chunk.iloc[x:y]
    chunk_.to_parquet("data_sample{}.snappy.parquet".format(x),compression='snappy')
# print(chunk.head())
# print(chunk.dtypes)

