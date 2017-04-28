# coding=utf-8
from dmagellan.utils.py_utils.utils import add_attrs, rename, projdf


def block_table_part(ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                     l_out_attrs, r_out_attrs, l_prefix,
                     r_prefix):
    # join the tables
    res = ltable.merge(rtable, left_on=l_block_attr, right_on=r_block_attr)

    # get the cols to project & project
    lcol, rcol = l_key + '_x', r_key + '_y'
    res = projdf(res, [lcol, rcol])

    # rename the fk columns to conform with given prefix
    lcol, rcol = l_prefix + l_key, r_prefix + r_key
    res = rename(res, [lcol, rcol])

    # add the required output attrs.
    res = add_attrs(res, ltable, rtable, lcol, rcol, l_key, r_key, l_out_attrs,
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
    cdf = add_attrs(candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                    [l_block_attr],
                    [r_block_attr], l_prefix, r_prefix)
    l_chk, r_chk = l_prefix + l_block_attr, r_prefix + r_block_attr

    res = candset[cdf[l_chk] == cdf[r_chk]]

    return res
