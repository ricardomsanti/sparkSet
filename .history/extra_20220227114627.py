#%%
from alive_progress import alive_bar
import time as t
total=100
items = [x for x in range(20)]
with alive_bar(total) as bar:  # declare your expected total
    for item in items:         # <<-- your original loop
        print(item)            # process each item
        t.sleep(0.001)
        bar()                  # call `bar()` at the end

