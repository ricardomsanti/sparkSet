#%%
from alive_progress import alive_bar
import time as t

def progress(x):
    for x in 10,0:
        with alive_bar(x) as bar:
            for i in range(10):
                t.sleep(0.001)

l = [progress(x) for x in range(2)]

