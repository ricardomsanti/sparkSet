
        
def countDir(self): 
    dcFiles = dbutils.fs.ls(self.DIR)
    df = spark.createDataFrame(sc.parallelize(dcFiles))
    count = df.count()
    displayHTML("DIRECTORY SET AS: ...  %s" % self.DIR)
    displayHTML("TOTAL OF FILES FOUND AFTER SCANNING : ... %s" % count)   

def graphDir(self):
    dcFiles = dbutils.fs.ls(self.DIR)
    df = spark.createDataFrame(sc.parallelize(dcFiles))
    df = df.sort(df.size)
    count = df.count()
    display(df)

def descDir(self):
    #origin
    dcFiles = dbutils.fs.ls(self.dc)
    df = spark.createDataFrame(sc.parallelize(dcFiles))
    df = df.describe()
    display(df)

def filesDir(self):
    #origin
    dcFiles = dbutils.fs.ls(self.DIR)
    return dcFiles       
    


