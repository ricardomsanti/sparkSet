#%%
from alive_progress import alive_bar
import time as t

def pb1(total=None, items=None):
    with alive_bar(total) as bar:  
        for item in items:         
            print(item)            
            t.sleep(0.01)
            bar()     

def pb2(time=0.001, barMaxList=None, range=None):  
    for x in barMaxList:
        with alive_bar(x) as bar:
            for i in range(1000):
                t.sleep(time)
                bar()

pb2(barMaxList=[100,20,0], range=100)