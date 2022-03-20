# %%
import concurrent.futures
import time as t

# %%
from alive_progress import alive_bar
import time as t

def pb1(total=None, items=None):
    with alive_bar(total) as bar:  
        for item in items:         
            print(item)            
            t.sleep(0.01)
            bar()     

def pb2(time=0.001, barMaxList=None, rg=None):  
    for x in barMaxList:
        with alive_bar(x) as bar:
            for i in range(rg):
                t.sleep(time)
                bar()


# %%


# %%
import sys
WORDS = [x for x in "ABCDEF"]
WORDS2 = []

#FUNCTION 1--------------------------
def maxRunWorkerTask(param=None):
    t.sleep(2)
    #TRANSFORMATION FUNC
    WORDS2.append(word)
    print(len(WORDS2))
    #sys.stdout.flush()
#FUNCTION 2--------------------------
def maxRunWorker(max_workers=4, mode="submit")
    if mode == "submit":
with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
    executor.submit(changeC, WORDS)

print(len(WORDS2))
    
       


