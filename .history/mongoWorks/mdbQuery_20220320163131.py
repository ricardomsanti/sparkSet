\# %%

#%%

import pandas as pd
from pymongo import MongoClient
from datetime import datetime, timedelta
import concurrent.futures
import sys
import os.path
import bson as b



class mdbQuery():
    def __init__(self, uri ="", dbName="", colName=""):
        self.uri_string = uri
        self.dbName = dbName
        self.colName = colName
        self.client = MongoClient(self.uri_string)
        self.database = self.client.get_database(self.db_name)
        self.col = self.getCol()
        

    def getCol(self):
        col = self.client\
            .db.get_collection(self.colName)
        return col

uri = "mongodb://localhost:27017/?readPreference=primary&appname=MongoDB%20Compass&directConnection=true&ssl=false"
db = mdbQuery(uri, 'local', 'SPT')
for x in db.col.find():
    print(x)

#%%ICIAIS 
# TOTAL DE ITENS DA QUERY
# TOTAL DE CORES
# NÚMERO DE CORES A SER UTILIZADO
# NÚMERO DE SUBLISTAS A SER CRIADO

TT_COL = col.find({},{"_id"}).count()
TT_COL
CORES = os.cpu_count()
CUSTOM_CORES = 4
DIV = TT_COL/CUSTOM_CORES
DIV



# USANDO A LISTA MAIOR
#----------------------------------------------------------------------------------------------------------------------------------------------------------------

##################################################################################################################################################################
#                             #         #       #         #         #                                                               #        #                   #
#    X  /  B.DECODE_ALL(X)    #   FOR   #   X   #    IN   #   col.  #  FIND / AGGREGATE / FIND_RAW_BATCHES / AGGREGATE_RAW_BATCHES  # LIMIT  #  SLICER [ : : ]   #
#                             #         #       #         #         #                                                               #        #                   #
##################################################################################################################################################################


# CRIA-SE UMA LISTA DE LISTAS MENORES
#---------------------------------------------------------------------------------------


# AS LISTAS MENOES SERVIRÃO DE PARÂMETRO PARA  FUNÇÃO DE TRANSFORMAÇÃO
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

# A FUNÇÃO DE TRANSFORMAÇÃO , JUNTO COM A LISTA DE LISTAS  SERVIRÃO DE PARÂMETRO PARA A FUNÇAO FINAL 
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

 


