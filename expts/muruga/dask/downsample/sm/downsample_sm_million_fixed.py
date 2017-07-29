import pandas as pd
import time
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan')

from dmagellan.sampler.downsample.downsample import downsample_sm
from dmagellan.utils.py_utils.utils import *
import psutil
from dask import multiprocessing, threaded
from dask.diagnostics import ProgressBar, Profiler, ResourceProfiler, CacheProfiler, visualize
import cloudpickle
import py_entitymatching as em

pbar = ProgressBar()
pbar.register()

import os
datapath='../datasets'


stopwords = get_stop_words(os.path.join(datapath, 'stopwords'))
#stopwords.extend(['and', 'in', 'the', 'my', 'me', 'to', 'you', 'i', 'andre', 'from', 'a', 'of', 'the', 'version', 'love', 'live', 'la', 'mix', 'album', 'dont'])
stopwords = list(set(stopwords))


print("Mem. usage before reading:{0} (GB)".format( psutil.virtual_memory().used/1e9))
A = pd.read_csv('../datasets/msd.csv')
B =  pd.read_csv('../datasets/msd.csv')
print("Mem. usage after reading:{0} (GB)".format(psutil.virtual_memory().used/1e9))

#stopWords = list(get_stop_words())

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
C = downsample_sm(A, B, 'id', 'id', 100000, 1, lstopwords=stopwords, rstopwords=stopwords)
#_= C.compute(get=threaded.get)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after downsampling): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))
