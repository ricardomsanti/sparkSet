#%%

import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta

URI = "mongodb://localhost:27017/"
CLIENT = MongoClient(URI)
DATABASE = CLIENT.get_database("local")
COLLENTION = DATABASE.get_collection("SPT")
QUERY = COLLENTION.find()


def mongoTestQuery():
    QUERY = COLLENTION.find()
    T0 = datetime.now()
    DATA_SIZE = 63000
    QUERY_LIST = list(QUERY.limit(DATA_SIZE))

    ct  =[ x for x in QUERY_LIST]

    T1 = datetime.now()
    MEMORY_LIST = []
    for x in ct:
        MEMORY_LIST.append(x)

    print(len(ct))
    print(f"RESULTS: DATA_SIZE = {DATA_SIZE}, QUERY_TIME = {(T1-T0).seconds} seconds")
    #%%
mongoTestQuery()