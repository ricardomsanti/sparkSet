#%%
from alive_progress import alive_bar
import time

def progress(x):
    for x in 10,0:
        with alive_bar(x) as bar:
            #for i in range(10):
            time.sleep(.005)

l = [progress(x) for x in range(100)]

