import pandas as pd
import time
import sys
#sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
import psutil
from dask import multiprocessing, threaded
from distributed import Client
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle

pbar = ProgressBar()
pbar.register()
client = Client('127.0.0.1:8786')

print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/sample_msd_100k.csv')
B = pd.read_csv('./datasets/sample_msd_100k.csv')

ob = OverlapBlocker()
memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
print("Mem. usage before reading:{0}".format(memUsageBefore))
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', overlap_size=2, rem_stop_words=True, compute=False, nltable_chunks=1, nrtable_chunks=4, l_output_attrs=['title'], r_output_attrs=['title'])

D = C.compute(get=client.get)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print(len(D))
print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time diff: {0}'.format(timeAfter-timeBefore))





