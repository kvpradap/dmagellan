import pandas as pd
import time
import sys
#sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
sys.path.append('/scratch/pradap/python-work/dmagellan')

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
import psutil
from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
from distributed import Client
import cloudpickle


client = Client('127.0.0.1:8786')
pbar = ProgressBar()
pbar.register()
print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('../datasets/sample_citeseer_100k.csv')
B = pd.read_csv('../datasets/sample_dblp_100k.csv')
print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
ob = OverlapBlocker()
# print("Mem. usage before reading:{0}", memUsageBefore)
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', overlap_size=1, compute=False, nltable_chunks=5, nrtable_chunks=5, rem_stop_words=True)

D = C.compute(get=client.get)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0}'.format(timeAfter-timeBefore))




