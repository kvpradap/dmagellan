import psutil
import time
import sys
sys.path.append('/scratch/pradap/python-work/anhaidgroup/py_entitymatching')

import py_entitymatching as em
import logging
logging.basicConfig(level=logging.INFO)
print("Mem. usage before reading:{0} (GB)".format( psutil.virtual_memory().used/1e9))
A = em.read_csv_metadata('../datasets/sample_msd_100k.csv', key='id')
B = em.read_csv_metadata('../datasets/sample_msd_100k.csv', key='id')
C = em.read_csv_metadata('../datasets/candset_msd_300k.csv', key='_id', ltable=A, rtable=B, fk_ltable='l_id', fk_rtable='r_id')
print("Mem. usage after reading:{0} (GB)".format(psutil.virtual_memory().used/1e9))
len(C)

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
feature_table = em.get_features_for_matching(A, B)

memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
feature_vecs = em.extract_feature_vecs(C, feature_table=feature_table)
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after extract featvecs): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))
