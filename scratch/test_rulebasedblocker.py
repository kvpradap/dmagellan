# coding=utf-8
import os

import pandas as pd
import dask
from dask.threaded import get

from dmagellan.blocker.rulebased.rule_based_blocker import RuleBasedBlocker
from dmagellan.feature.autofeaturegen import get_features_for_blocking
# from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker


datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)


# A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
# B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

print('Reading the files done')
# rb = AttrEquivalenceBlocker()
# bb = BlackBoxBlocker()
block_f = get_features_for_blocking(A, B)
rb = RuleBasedBlocker()
# Add rule : block tuples if name_name_lev(ltuple, rtuple) < 0.4
_ = rb.add_rule(['name_name_lev_sim(ltuple, rtuple) < 0.1'], block_f)
rb.set_table_attrs(['name'], ['name'])

A['ID'] = A['ID'] + 10
C = rb.block_tables(A, B, 'ID', 'ID',
                    l_output_attrs=['name', 'address'],
                    r_output_attrs=['name', 'address'],
                    compute=False
                    )
D = rb.block_candset(C, A, B, 'l_ID', 'r_ID', "ID", "ID", nchunks=4,
                    compute=True, scheduler=get)

print(len(D))
