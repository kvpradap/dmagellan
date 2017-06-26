import pandas as pd
import time
import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
import psutil
from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle
filename='./profres_exp_mp_dblp_300k_overlapblocker.html'

pbar = ProgressBar()
pbar.register()

#print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/sample_citeseer_300k.csv')
B = pd.read_csv('./datasets/sample_dblp_300k.csv')
#print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

memUsageBefore = psutil.virtual_memory().used/1e9
ob = OverlapBlocker()
# print("Mem. usage before reading:{0}", memUsageBefore)
C = ob.block_tables(A.sample(1000), B.sample(1000), 'id', 'id', 'title', 'title', overlap_size=2, compute=False, nltable_chunks=1, nrtable_chunks=2)

with Profiler() as prof, CacheProfiler() as cprof, ResourceProfiler(dt=0.25) as rprof:
    D = C.compute(get=multiprocessing.get, num_workers=4)
memUsageAfter = psutil.virtual_memory().used/1e9
visualize([prof, cprof, rprof], file_path=filename, show=False)

#print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))




