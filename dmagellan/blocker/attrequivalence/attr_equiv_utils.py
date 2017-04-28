# coding=utf-8
from dmagellan.utils.py_utils.utils import add_attributes, rename_cols, proj_df


def block_table_part(ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                     l_out_attrs, r_out_attrs, l_prefix,
                     r_prefix):
    # join the tables
    ltbl = proj_df(ltable, [l_key, l_block_attr])
    rtbl = proj_df(rtable, [r_key, r_block_attr])
    res = ltbl.merge(rtbl, left_on=l_block_attr, right_on=r_block_attr)

    # get the cols to project & project
    lcol, rcol = l_key + '_x', r_key + '_y'
    res = proj_df(res, [lcol, rcol])

    # rename_cols the fk columns to conform with given prefix
    lcol, rcol = l_prefix + l_key, r_prefix + r_key
    res = rename_cols(res, [lcol, rcol])

    # add the required output attrs.
    res = add_attributes(res, ltable, rtable, lcol, rcol, l_key, r_key, l_out_attrs,
                         r_out_attrs,
                         l_prefix, r_prefix)

    # finally return the result.
    return res


def block_candset_part(candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                       l_block_attr, r_block_attr):
    # 1. create dummy column names to contain the values pulled from ltable and rtable
    # based on the fk's

    l_prefix, r_prefix = '__blk_a_', '__blk_b_'

    # add attrs
    cdf = add_attributes(candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                         [l_block_attr],
                         [r_block_attr], l_prefix, r_prefix)
    l_chk, r_chk = l_prefix + l_block_attr, r_prefix + r_block_attr

    res = candset[cdf[l_chk] == cdf[r_chk]]

    return res
