import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/anhaid/py_entitymatching')

import py_entitymatching as em
import os

from dask import get
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching
from dmagellan.matcher.dtmatcher import DTMatcher

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
IJ = em.split_train_test(S, train_proportion=0.5, random_state=0)
I = IJ['train']
J = IJ['test']

F = get_features_for_matching(A, B)
H = extract_feature_vecs(I, A, B, '_id',  'ltable_id', 'rtable_id', 'id', 'id',
                                 feature_table=F, attrs_after='label', nchunks=4,
                                                                    compute=True,
                         scheduler=get)
print(len(H))
# print(L.head())



# Instantiate the matcher to evaluate.
dt = DTMatcher(name='DecisionTree', random_state=0)
# Train using feature vectors from I
dt.fit(table=H,
       exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'label'],
       target_attr='label')

# Convert J into a set of feature vectors using F
L = extract_feature_vecs(J, A, B, '_id',  'ltable_id', 'rtable_id', 'id', 'id', nchunks=4,
                            feature_table=F, attrs_after='label', show_progress=False,
                         compute=False)

# print(len(L))
# print(L.head(1))
predictions = dt.predict(table=L, exclude_attrs=['_id', 'ltable_id', 'rtable_id', 'label'],
              append=True, target_attr='predicted', inplace=False, nchunks=2,
                         compute=False)
from dmagellan.optimization.exfeatvecs_predict_sequence_opt import delay_concat, fuse_dag
opt1 = delay_concat(dict(predictions.dask))
opt2 = fuse_dag(opt1)

from dask.dot import dot_graph
dot_graph(opt2)
# print(predictions.head())
# predictions.visualize()

