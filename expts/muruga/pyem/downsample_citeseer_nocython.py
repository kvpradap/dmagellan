import psutil
import time


import py_entitymatching as em
print("Mem. usage before reading:{0} (GB)".format( psutil.virtual_memory().used/1e9))
A = em.read_csv_metadata('../datasets/citeseer.csv', key='id')
B = em.read_csv_metadata('../datasets/dblp.csv', key='id')
print("Mem. usage after reading:{0} (GB)".format(psutil.virtual_memory().used/1e9))

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
C = em.down_sample(A.sample(10000), B.sample(10000), 1000, 1, show_progress=True)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after downsampling): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))

