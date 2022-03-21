
#%%
import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import concurrent.futures
import sys
import os.path
import bson as b



class mdbSet():
    def __init__(self, uri ="", dbName="", colName=""):
        self.uri_string = uri
        self.dbName = dbName
        self.colName = colName
        self.client = MongoClient(self.uri_string)
        self.database = self.client.get_database(self.dbName)
        self.col = self.database.get_collection(self.colName)


uri = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
db =mdbSet(uri, 'local', 'SPT')

def colCount():
    colCount =  db.col.find({},{"_id"}).count()
    return colCount

def colFind(limit=""):
    if limit == "":
        colList = [ x for x in db.col.find()]
    else:
        colList = [ x for x in db.col.find().limit(limit)]
    return(colList)

def colRawFind(limit=""):
    if limit == "":
        colList = [ x for x in db.col.find_raw_batches()]
    else:
        colList = [ b.decode_all(x) for x in db.col.find_raw_batches().limit(limit)]
    return(colList)

def colFindID():
    idList = [str(x).split("'",4)[3] for x in db.col.find({},{"_id":1}).limit(20)]
    return(idList)
    
    
def maxRunWorkerTask(ID=None):
    #TRANSFORMATION FUNC
    query = db.col.aggregate(
                                [  
                                    {   
                                        '$match' : 
                                                    {
                                                        '_id': ID
                                                    }
                                    }
                                ]
                            )
    RESULT_LIST.append(query)
    

def maxRunWorker(max_workers=4, mode="submit"):    
    ID_LIST = colFindID()
    T0 = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        if mode == "submit":
            for x in ID_LIST:
                executor.submit(maxRunWorkerTask, ID=x)
        else:
            executor.map(maxRunWorkerTask, ID_LIST)
        
    T1 = datetime.now()
    print(f" QUERY_TIME = {(T1-T0).seconds} seconds")

RESULT_LIST = []

#maxRunWorker()
idList = colFindID()
for x in idList:
    maxRunWorkerTask(x)
RESULT_LIST