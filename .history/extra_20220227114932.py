#%%
from alive_progress import alive_bar
import time as t

def pb1(total=None, items=None):
    with alive_bar(total) as bar:  
        for item in items:         
            print(item)            
            t.sleep(0.01)
            bar()     

def pb2():  
    for x in 1000, 700, 0:
        with alive_bar(x) as bar:
            for i in range(1000):
                t.sleep(.005)
                bar()

pb2