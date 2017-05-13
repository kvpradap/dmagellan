import os

import pandas as pd
import dask

from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker
from dmagellan.feature.extractfeatures import extract_feature_vecs
from dmagellan.feature.autofeaturegen import get_features_for_matching

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

# A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

print('Reading the files done')
ab = AttrEquivalenceBlocker()
C = ab.block_tables(A, B, 'ID', 'ID', 'birth_year', 'birth_year', ['name', 'address',
                                                                   'zipcode'],
                    ['name', 'address', 'zipcode'], nltable_chunks=2, nrtable_chunks=2,
                    compute=True, scheduler=dask.get
                    )
match_f = get_features_for_matching(A, B)
feat_vecs = extract_feature_vecs(C, A, B, '_id',  'l_ID', 'r_ID', 'ID', 'ID',
                                 feature_table=match_f, nchunks=2, compute=False)
from dask.dot import dot_graph
feat_vecs.visualize()
# print(feat_vecs.head())