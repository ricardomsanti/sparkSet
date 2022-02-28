#%%

import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

URI = "mongodb://localhost:27017/"
CLIENT = MongoClient(URI)
DATABASE = CLIENT.get_database("local")
COLLENTION = DATABASE.get_collection("SPT")
QUERY = COLLENTION.find()

T0 = datetime.now()

QUERY_LIST = list(QUERY.limit(10))

df = pd.DataFrame(QUERY_LIST)
ct  =[ x for x in QUERY_LIST]

T1 = datetime.now()
with open('TEXT1.txt', 'w', encoding='utf-8') as f:
    for x in QUERY_LIST:
        w = f.write(x)
print(len(ct))
print(f"TOTAL TIME: {(T1-T0).microseconds} microseconds")
