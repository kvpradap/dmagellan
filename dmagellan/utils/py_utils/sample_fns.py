# from dask import delayed
# import pandas as pd
# @delayed
# def split_df(table, n):
#     return [0]*n
# @delayed
# def concat_df(tables):
#     return pd.DataFrame()
# @delayed
# def _attr_block_candset(ltable, rtable, l_block_attr, r_block_attr):
#     return pd.DataFrame()
#
# @delayed
# def _overlap_block_candset(ltable, rtable, l_block_attr, r_block_attr):
#     return pd.DataFrame()
#
# @delayed
# def attr_block_candset(candset, l_block_attr, r_block_attr, nchunks):
#     cand_splitted = split_df(candset, nchunks)
#     results = []
#     for i in range(nlchunks):
#         res = _attr_block_candset(cand_splitted[i], l_block_attr, r_block_attr)
#         results.append(res)
#     fin = concat_df(results)
#     return fin
#
# @delayed(name='overlap')
# def overlap_block_candset(candset, l_block_attr, r_block_attr, nchunks):
#     cand_splitted = split_df(candset, nchunks)
#     results = []
#     for i in range(nlchunks):
#         res = _attr_block_candset(cand_splitted[i], l_block_attr, r_block_attr)
#         results.append(res)
#     fin = concat_df(results)
#     return fin
#
#

import os

import pandas as pd
import dask

from dmagellan.blocker.attrequivalence.attr_equiv_blocker import AttrEquivalenceBlocker
from dmagellan.blocker.overlap.overlapblocker import OverlapBlocker
from dmagellan.blocker.blackbox.blackbox_blocker import BlackBoxBlocker
from dmagellan.blocker.rulebased.rule_based_blocker import RuleBasedBlocker
from dmagellan.feature.autofeaturegen import get_features_for_blocking
#

def test_create_dag():
    datapath = "/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan/datasets"
    A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
    B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)

    # A = pd.read_csv(os.path.join(datapath, 'tracks.csv'), low_memory=False)
    # B = pd.read_csv(os.path.join(datapath, 'songs.csv'), low_memory=False)

    print('Reading the files done')
    ab = AttrEquivalenceBlocker()
    C = ab.block_tables(A, B, 'ID', 'ID', 'birth_year', 'birth_year', ['name', 'address',
                                                                       'zipcode'],
                        ['name', 'address', 'zipcode'], nltable_chunks=2, nrtable_chunks=2,
                        compute=False, scheduler=dask.get
                        )

    def last_name_match(ltuple, rtuple):
        l_first_name, l_last_name = ltuple['name'].split()
        r_first_name, r_last_name = rtuple['name'].split()
        return l_last_name != r_last_name

    bb = BlackBoxBlocker()
    bb.set_black_box_function(last_name_match)
    bb.set_ltable_attrs(['name'])
    bb.set_rtable_attrs(['name'])
    D = bb.block_candset(C, A, B, 'l_ID', 'r_ID', "ID", "ID", nchunks=4,
                        compute=False, scheduler=dask.get)

    ob = OverlapBlocker()
    E = ob.block_candset(D, A, B, "l_ID", "r_ID", "ID", "ID", 'name', 'name',
                         nchunks=4, overlap_size=1, compute=False)

    block_f = get_features_for_blocking(A, B)
    rb = RuleBasedBlocker()
    # Add rule : block tuples if name_name_lev(ltuple, rtuple) < 0.4
    _ = rb.add_rule(['name_name_lev_sim(ltuple, rtuple) < 0.4'], block_f)
    rb.set_table_attrs(['name'],['name'])

    F = rb.block_candset(E, A, B, 'l_ID', 'r_ID', "ID", "ID", nchunks=4,
                        compute=False, scheduler=dask.get)

    return F