# coding=utf-8
import sys
sys.path.append('/scratch/pradap/python-work/dmagellan')
import py_entitymatching as em
import os
import time
import psutil
import pandas as pd

from dask import get, multiprocessing
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching
from dmagellan.matcher.dtmatcher import DTMatcher
from dmagellan.matcher.svmmatcher import SVMMatcher
from dmagellan.matcher.rfmatcher import RFMatcher
from dmagellan.matcher.logregmatcher import LogRegMatcher
from dmagellan.matcher.nbmatcher import NBMatcher
from dmagellan.matcher.linregmatcher import LinRegMatcher

from dmagellan.mlmatcherselection.mlmatcherselection import select_matcher

# Get the datasets directory
print("Mem. usage before reading:{0}".format( psutil.virtual_memory().used/1e9))
path_feat_vecs = '../datasets/featvecs_citeseer.csv'
H = pd.read_csv(path_feat_vecs)
H.fillna(value=0, inplace=True)
print("Mem. usage after reading:{0}".format(psutil.virtual_memory().used/1e9))


memUsageBefore = psutil.virtual_memory().used/1e9
timeBefore = time.time()
# Create a set of ML-matchers
dt = DTMatcher(name='DecisionTree', random_state=0)
svm = SVMMatcher(name='SVM', random_state=0)
rf = RFMatcher(name='RF', random_state=0)
lg = LogRegMatcher(name='LogReg', random_state=0)
nb = NBMatcher(name='NaiveBayes')
ln = LinRegMatcher(name='LinearRegression')


# Impute feature vectors with the mean of the column values.
# H = em.impute_table(H,
#                 exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'label'],
#                 strategy='mean')
# Select the best ML matcher using CV
result = select_matcher([dt, lg, svm, rf, nb, ln], table=H,
        exclude_attrs=['_id', 'l_id', 'r_id', 'label'],
        k=len(H)-1,
        target_attr='label', metric='f1', random_state=0, compute=False)
result = result.compute(get=multiprocessing.get)
# print(result['cv_stats'])
# result.visualize()
timeAfter = time.time()
memUsageAfter = psutil.virtual_memory().used/1e9

print('Mem.usage (after reading): {0}, Mem.usage (after sel. matcher): {1}, diff: {2}'.format(memUsageBefore, memUsageAfter, memUsageAfter-memUsageBefore))
print('Time. diff: {0}'.format(timeAfter-timeBefore))
