#%%


from pymongo import MongoClient
URI = "mongodb://localhost:27017/"
CLIENT = MongoClient(URI)
DATABASE = CLIENT.get_database("local")
COLLENTION = DATABASE.get_collection("SPT")
QUERY = COLLENTION.find()
QUERY_LIST = list(QUERY)
for x in QUERY_LIST:
    print(x)