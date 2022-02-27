#%%
from alive_progress import alive_bar
import time

for x in range(1000):
   with alive_bar(x) as bar:
       for i in range(1000):
           time.sleep(.005)
           bar()

