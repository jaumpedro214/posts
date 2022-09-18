import pandas as pd
import pymongo
import time
import pathlib


# import data from accidents.parquet
# and insert one entry at a time
# into mongodb collection accidents.accidents_bronze

df_folder = './accidents.parquet'
files = pathlib.Path(df_folder).glob("*.parquet")


# connect to mongodb
# user - mongo; password - mongo
# database - accidents; collection - accidents_bronze
# replica set - replica-set

SLEEP_TIME = 0.01

client = pymongo.MongoClient(
    "mongodb://mongo:mongo@localhost:27017"
)

collection = client["accidents"]["accidents_bronze"]

for path in files:
    df = pd.read_parquet(
        path
    )
    
    for index, row in df.iterrows():
        print("Inserting entry: ", index)
        collection.insert_one(
            row.to_dict()
        )
        
        print(row)
        print("Row successfully inserted.")
        
        time.sleep(SLEEP_TIME)
