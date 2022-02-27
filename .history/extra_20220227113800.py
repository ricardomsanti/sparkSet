#%%
from alive_progress import alive_bar
import time

for x in range(10):
   with alive_bar(x) as bar:
       #for i in range(10):
        time.sleep(.005)
        bar()

