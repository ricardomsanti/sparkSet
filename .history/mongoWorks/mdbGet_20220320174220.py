\# %%

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
    idList = [ x["_id"] for x in colFindID()]
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
                                    },  
                                    {
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
                executor.submit(maxRunWorkerTask, param=x)
        else:
            executor.map(maxRunWorkerTask, ID_LIST)
        
    T1 = datetime.now()
    print(f" QUERY_TIME = {(T1-T0).seconds} seconds")

RESULT_LIST = []
IDS = colFindID()
#maxRunWorker(2)
print(IDS)
#%%

# USANDO A LISTA MAIOR
#----------------------------------------------------------------------------------------------------------------------------------------------------------------

##################################################################################################################################################################
#                             #         #       #         #         #                                                               #        #                   #
#    X  /  B.DECODE_ALL(X)    #   FOR   #   X   #    IN   #   col.  #  FIND / AGGREGATE / FIND_RAW_BATCHES / AGGREGATE_RAW_BATCHES  # LIMIT  #  SLICER [ : : ]   #
#                             #         #       #         #         #                                                               #        #                   #
##################################################################################################################################################################


# CRIA-SE UMA LISTA DE LISTAS MENORES
#---------------------------------------------------------------------------------------


# AS LISTAS MENOES SERVIR??O DE PAR??METRO PARA  FUN????O DE TRANSFORMA????O
#################
#maxRunWorkerTask
################

def maxRunWorkerTask(param=None):
    #TRANSFORMATION FUNC
    #QUERY = COLLECTION.aggregate([{ '$match' : {'_id': ID},{'$project' :{}}}])
    QUERY = [x for x in COLLECTION.aggregate([{ '$match' : {'$_id': param}}])] 
    qf = queryFormat(q=QUERY)
    with open(path='TEXT1.txt', mode='w', encode='utf-8') as f:
        w = f.writelines(QUERY)
#-------------------------------------------------------------------------------

# A FUN????O DE TRANSFORMA????O , JUNTO COM A LISTA DE LISTAS  SERVIR??O DE PAR??METRO PARA A FUN??AO FINAL 
#################
#maxRunWorker
################


def maxRunWorker(max_workers=4, mode="submit"):    
    T0 = datetime.now()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        if mode == "submit":
            for x in ID_LIST:
                executor.submit(maxRunWorkerTask, param=x)
        else:
            executor.map(maxRunWorkerTask, ID_LIST)
        
    T1 = datetime.now()
    print(f" QUERY_TIME = {(T1-T0).seconds} seconds")
#-------------------------------------------------------------------------------


# %%



ID_LIST  = [x for x in COLLECTION.find({},{"_id"}).count()
FINAL_ID a= []
print(len(ID_LIST))
print(len(FINAL_ID))



#FUNCTION 1--------------------------
##########################################
##########################################

def maxRunWorkerTask(param=None):
    #TRANSFORMATION FUNC
    #QUERY = COLLECTION.aggregate([{ '$match' : {'_id': ID},{'$project' :{}}}])
    QUERY = [x for x in COLLECTION.aggregate([{ '$match' : {'$_id': param}}])] 
    qf = queryFormat(q=QUERY)
    with open(path='TEXT1.txt', mode='w', encode='utf-8') as f:
        w = f.writelines(QUERY)



#FUNCTION 2--------------------------
##########################################
##########################################


maxRunWorker(mode="map")

 


