import pandas as pd
import time
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan')
import psutil

from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching

from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle

pbar = ProgressBar()
pbar.register()

print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('../datasets/sample_citeseer_100k.csv')
B = pd.read_csv('../datasets/sample_dblp_100k.csv')
print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))

C = pd.read_csv('../datasets/citeseer_candset_300k.csv')

feature_table = get_features_for_matching(A, B)

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
feature_vecs = extract_feature_vecs(C, A, B, '_id', 'l_id',  'r_id', 'id', 'id', feature_table=feature_table,
        nchunks=4, compute=False)
D = feature_vecs.compute(get=multiprocessing.get, num_workers=4)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after extract featvecs): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))
