from time import time
import pandas as pd
import pymongo
import datetime
import pathlib
import time

# import data from accidents.parquet

df_folder = './accidents.parquet'
files = list(pathlib.Path(df_folder).glob("*.parquet"))

# connect to mongodb
# user - mongo; password - mongo
# database - accidents; collection - accidents_bronze
# replica set - replica-set
client = pymongo.MongoClient(
    "mongodb://mongo:mongo@localhost:27017"
)

collection = client["accidents"]["accidents_bronze"]

for file_path in files:
    df = pd.read_parquet(
        file_path
    )
    
    # insert entries into mongodb
    collection.insert_many(
        df.to_dict(orient='records')
    )
    print(f"[{datetime.datetime.now()}] Inserted entries: {len(df)}")
    time.sleep(1)
    
    del df