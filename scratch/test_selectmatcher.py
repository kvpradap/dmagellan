import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/anhaid'
                '/py_entitymatching')

import py_entitymatching as em
import os

from dask import get
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
datasets_dir = em.get_install_path() + os.sep + 'datasets'

path_A = datasets_dir + os.sep + 'dblp_demo.csv'
path_B = datasets_dir + os.sep + 'acm_demo.csv'
path_labeled_data = datasets_dir + os.sep + 'labeled_data_demo.csv'

A = em.read_csv_metadata(path_A, key='id')
B = em.read_csv_metadata(path_B, key='id')
# Load the pre-labeled data
S = em.read_csv_metadata(path_labeled_data,
                         key='_id',
                         ltable=A, rtable=B,
                         fk_ltable='ltable_id', fk_rtable='rtable_id')
# Split S into I an J
IJ = em.split_train_test(S, train_proportion=0.5, random_state=0)
I = IJ['train']
J = IJ['test']

# Create a set of ML-matchers
dt = DTMatcher(name='DecisionTree', random_state=0)
svm = SVMMatcher(name='SVM', random_state=0)
rf = RFMatcher(name='RF', random_state=0)
lg = LogRegMatcher(name='LogReg', random_state=0)
nb = NBMatcher(name='NaiveBayes')
ln = LinRegMatcher(name='LinearRegression')
F = get_features_for_matching(A, B)
H = extract_feature_vecs(I, A, B, '_id',  'ltable_id', 'rtable_id', 'id', 'id',
                                 feature_table=F, attrs_after='label', nchunks=4,
                                                                    compute=True,
                         scheduler=get)
print(len(H))

H.fillna(value=0, inplace=True)
# Impute feature vectors with the mean of the column values.
# H = em.impute_table(H,
#                 exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'label'],
#                 strategy='mean')
# Select the best ML matcher using CV
result = select_matcher([dt, lg, svm, rf, nb, ln], table=H,
        exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'label'],
        k=5,
        target_attr='label', metric='f1', random_state=0, compute=True)
print(result['cv_stats'])
# result.visualize()