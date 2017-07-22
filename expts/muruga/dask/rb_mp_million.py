import pandas as pd
import time
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan')

from dmagellan.blocker.rulebased.rule_based_blocker import RuleBasedBlocker
from dmagellan.feature.autofeaturegen import get_features_for_blocking

import psutil
from dask import multiprocessing, threaded
from distributed import Client
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle

pbar = ProgressBar()
pbar.register()

print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('./datasets/sample_msd_100k.csv')
B = A

block_f = get_features_for_blocking(A, B)
rb = RuleBasedBlocker()
_ = rb.add_rule(['title_title_jac_dlm_dc0_dlm_dc0(ltuple, rtuple) < 0.8'])
rb.set_table_attrs(['title'], ['title'])
memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
print("Mem. usage before reading:{0}".format(memUsageBefore))
C = ob.block_tables(A, B, 'id', 'id', 'title', 'title', overlap_size=2, rem_stop_words=True, compute=False, nltable_chunks=1, nrtable_chunks=4, l_output_attrs=['title'], r_output_attrs=['title'])

_ = C.compute(get=multiprocessing.get)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0}, Mem.usage (after blocking): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time diff: {0}'.format(timeAfter-timeBefore))





