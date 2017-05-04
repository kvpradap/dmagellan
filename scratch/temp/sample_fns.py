from dask import delayed
import pandas as pd
@delayed
def split_df(table, n):
    return [0]*n
@delayed
def concat_df(tables):
    return pd.DataFrame()
@delayed
def _attr_block_candset(ltable, rtable, l_block_attr, r_block_attr):
    return pd.DataFrame()

@delayed
def _overlap_block_candset(ltable, rtable, l_block_attr, r_block_attr):
    return pd.DataFrame()

@delayed
def attr_block_candset(candset, l_block_attr, r_block_attr, nchunks):
    cand_splitted = split_df(candset, nchunks)
    results = []
    for i in range(nlchunks):
        res = _attr_block_candset(cand_splitted[i], l_block_attr, r_block_attr)
        results.append(res)
    fin = concat_df(results)
    return fin

@delayed(name='overlap')
def overlap_block_candset(candset, l_block_attr, r_block_attr, nchunks):
    cand_splitted = split_df(candset, nchunks)
    results = []
    for i in range(nlchunks):
        res = _attr_block_candset(cand_splitted[i], l_block_attr, r_block_attr)
        results.append(res)
    fin = concat_df(results)
    return fin