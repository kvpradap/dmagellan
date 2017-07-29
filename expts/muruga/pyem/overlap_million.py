import psutil
import time
import sys
sys.path.append('/scratch/pradap/python-work/anhaidgroup/py_entitymatching')
import py_entitymatching as em
import logging
logging.basicConfig(level=logging.INFO)
print("Mem. usage before reading:{0} (GB)".format( psutil.virtual_memory().used/1e9))
#A = em.read_csv_metadata('../datasets/sample_msd_200k.csv', key='id')
#B = em.read_csv_metadata('../datasets/sample_msd_200k.csv', key='id')
A = em.read_csv_metadata('../datasets/sample_msd_100k.csv', key='id')
B = em.read_csv_metadata('../datasets/sample_msd_100k.csv', key='id')
print("Mem. usage after reading:{0} (GB)".format(psutil.virtual_memory().used/1e9))

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
ob = em.OverlapBlocker()
C = ob.block_tables(A, B, 'title', 'title', overlap_size=2, rem_stop_words=True, n_jobs=-1, show_progress=True)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after downsampling): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))

