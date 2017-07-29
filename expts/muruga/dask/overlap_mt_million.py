import pandas as pd
import time
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan')

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
import psutil
from dask import multiprocessing, threaded
from distributed import Client
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle

pbar = ProgressBar()
pbar.register()

print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
#A = pd.read_csv('../datasets/sample_msd_200k.csv')
#B = pd.read_csv('../datasets/sample_msd_200k.csv')
A = pd.read_csv('../datasets/sample_msd_100k.csv')
B = pd.read_csv('../datasets/sample_msd_100k.csv')
print(len(A), len(B))
ob = OverlapBlocker()
memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
print("Mem. usage before reading:{0}".format(memUsageBefore))
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', overlap_size=2, rem_stop_words=True, compute=False, nltable_chunks=1, nrtable_chunks=4)

D = C.compute(get=threaded.get)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print(len(D))
print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time diff: {0}'.format(timeAfter-timeBefore))
#D.to_csv('../datasets/candset_msd_overlap_blocker_th2.csv', index=False)
#D.sample(300000).to_csv('../datasets/candset_msd_300k.csv', index=False)





