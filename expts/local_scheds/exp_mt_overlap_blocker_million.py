import pandas as pd
import time
import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')

from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
import psutil
from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle
filename='./profres_exp_mt_million_300k_overlapblocker.html'

pbar = ProgressBar()
pbar.register()

#print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/msd_300k.csv')
B = pd.read_csv('./datasets/msd_300k.csv')
#print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

ob = OverlapBlocker()
# print("Mem. usage before reading:{0}", memUsageBefore)
C = ob.block_tables(A.sample(10000), B.sample(10000), 'id', 'id', 'title', 'title', overlap_size=3, rem_stop_words=True, compute=False, nltable_chunks=1, nrtable_chunks=4)

with Profiler() as prof, CacheProfiler() as cprof, ResourceProfiler(dt=0.25) as rprof:
    D = C.compute(get=threaded.get, num_workers=4)
visualize([prof, cprof, rprof], file_path=filename, show=False)
print(len(D))
E = D.sample(500)
print('Writing to file')
E.to_csv('./datasets/candset_million.csv', index=False)

#print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))





