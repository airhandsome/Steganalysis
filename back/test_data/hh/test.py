import time
import os

start = time.time()
st = "/home/test_data/GFR -i 4.jpg  -o 4.txt"
os.system(st)
end=time.time()
print("running time is %d"%(end-start))
