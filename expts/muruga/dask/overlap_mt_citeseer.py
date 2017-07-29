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

pbar = ProgressBar()
pbar.register()
print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
#A = pd.read_csv('../datasets/sample_citeseer_200k.csv')
#B = pd.read_csv('../datasets/sample_dblp_200k.csv')
A = pd.read_csv('../datasets/sample_citeseer_100.csv')
B = pd.read_csv('../datasets/sample_dblp_100.csv')
print(len(A), len(B))
print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
ob = OverlapBlocker()
# print("Mem. usage before reading:{0}", memUsageBefore)
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', overlap_size=2, compute=False, nltable_chunks=1, nrtable_chunks=4, rem_stop_words=True, l_output_attrs=['title'], r_output_attrs=['title'])

D = C.compute(get=threaded.get)
len(D)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0}'.format(timeAfter-timeBefore))
print(len(D))
#D.sample(300000).to_csv('../datasets/citeseer_candset_300k.csv', index=False)


D.to_csv('./mur_candset_dask.csv', index=False)




