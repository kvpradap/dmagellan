import psutil
import time
import sys
import py_entitymatching as em
import logging
logging.basicConfig(level=logging.INFO)
print("Mem. usage before reading:{0} (GB)".format( psutil.virtual_memory().used/1e9))
A = em.read_csv_metadata('../datasets/sample_citeseer_100k.csv', key='id')
B = em.read_csv_metadata('../datasets/sample_dblp_100k.csv', key='id')
C = em.read_csv_metadata('../datasets/featvecs_citeseer.csv', key='_id', ltable=A, rtable=B, fk_ltable='l_id', fk_rtable='r_id')
C.fillna(value=0, inplace=True)

print("Mem. usage after reading:{0} (GB)".format(psutil.virtual_memory().used/1e9))
len(C)

dt = em.DTMatcher(name='DecisionTree', random_state=0)
svm = em.SVMMatcher(name='SVM', random_state=0)
rf = em.RFMatcher(name='RF', random_state=0)
lg = em.LogRegMatcher(name='LogReg', random_state=0)
nb = em.NBMatcher(name='NaiveBayes')
ln = em.LinRegMatcher(name='LinearRegression')
memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
result = em.select_matcher([dt, lg, svm, rf, nb, ln], table=C,
        exclude_attrs=['_id', 'l_id', 'r_id', 'label'],
        k=len(C)-1,
        target_attr='label', metric='f1', random_state=0 )

timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0} (GB), Mem.usage (after extract featvecs): {1} (GB), diff: {2} (GB)'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0} (secs)'.format(timeAfter-timeBefore))
