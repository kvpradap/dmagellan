# coding=utf-8

from dask import threaded, delayed

from dmagellan.blocker.blocker_utils import get_attrs_to_project, \
    get_lattrs_to_project, get_rattrs_to_project
from dmagellan.utils.py_utils.utils import add_attributes, rename_cols
from dmagellan.utils.py_utils.utils import split_df, proj_df, concat_df, add_id, \
    exec_dag, lsplit_df, rsplit_df, candsplit_df, lproj_df, rproj_df, candproj_df

import pandas as pd

class AttrEquivalenceBlocker(object):
    def _block_table_part(self, ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                          l_out_attrs, r_out_attrs, l_prefix,
                          r_prefix):

        l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr, l_out_attrs)
        r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr, r_out_attrs)

        ltbl = (lproj_df)(ltable, l_proj_attrs)
        rtbl = (rproj_df)(rtable, r_proj_attrs)

        # join the tables
        # ltbl = lproj_df(ltable, [l_key, l_block_attr])
        # rtbl = rproj_df(rtable, [r_key, r_block_attr])
        res = ltbl.merge(rtbl, left_on=l_block_attr, right_on=r_block_attr)

        # get the cols to project & project
        lcol, rcol = l_key + '_x', r_key + '_y'
        res = candproj_df(res, [lcol, rcol])

        # rename_cols the fk columns to conform with given prefix
        lcol, rcol = l_prefix + l_key, r_prefix + r_key
        res = rename_cols(res, [lcol, rcol])

        # add the required output attrs.
        res = add_attributes(res, ltable, rtable, lcol, rcol, l_key, r_key, l_out_attrs,
                             r_out_attrs,
                             l_prefix, r_prefix)

        # finally return the result.
        if not isinstance(res, pd.DataFrame):
            print('Returning {0}'.format(res))
        return res

    def _block_candset_part(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                            r_key,
                            l_block_attr, r_block_attr):
        # 1. create dummy column names to contain the values pulled from ltable and rtable
        # based on the fk's

        if isinstance(candset, pd.DataFrame) and len(candset):

            l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr)
            r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr)

            ltable = (lproj_df)(ltable, l_proj_attrs)
            rtable = (rproj_df)(rtable, r_proj_attrs)


            l_prefix, r_prefix = '__blk_a_', '__blk_b_'

            # add attrs
            cdf = add_attributes(candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                                 [l_block_attr],
                                 [r_block_attr], l_prefix, r_prefix)
            l_chk, r_chk = l_prefix + l_block_attr, r_prefix + r_block_attr

            res = candset[cdf[l_chk] == cdf[r_chk]]

            if not isinstance(res, pd.DataFrame):
                print('Returning {0}'.format(res))

            return res
        else:
            return candset

    def block_tables(self, ltable, rtable, l_key, r_key, l_block_attr, r_block_attr,
                     l_output_attrs=None, r_output_attrs=None, l_output_prefix='l_',
                     r_output_prefix='r_',
                     nltable_chunks=1, nrtable_chunks=1, scheduler=threaded.get,
                     num_workers=None, cache_size=1e9, compute=False, show_progress=True):
        # @todo validate inputs
        # @todo need to handle missing values.

        ltable_splitted = (lsplit_df)(ltable, nltable_chunks)
        rtable_splitted = (rsplit_df)(rtable, nrtable_chunks)

        # l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr, l_output_attrs)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr, r_output_attrs)

        # list ot accomodate results
        results = []
        for i in xrange(nltable_chunks):
            # ltbl = (lproj_df)(ltable_splitted[i], l_proj_attrs)
            for j in xrange(nrtable_chunks):
                # rtbl = (rproj_df)(rtable_splitted[j], r_proj_attrs)
                res = delayed(self._block_table_part)(ltable_splitted[i],
                                                      rtable_splitted[j],
                                                      l_key,
                                                      r_key,
                                                      l_block_attr,
                                                      r_block_attr, l_output_attrs,
                                                      r_output_attrs,
                                                      l_output_prefix, r_output_prefix)
                results.append(res)
        candset = delayed(concat_df)(results)
        candset = delayed(add_id)(candset)

        if compute:
            candset = exec_dag(candset, num_workers, cache_size, scheduler,
                               show_progress)

        return candset

    def block_candset(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                      r_key, l_block_attr, r_block_attr, nchunks=1,
                      scheduler=threaded.get,
                      num_workers=None, cache_size=1e9, compute=False,
                      show_progress=True):
        cand_splitted = delayed(candsplit_df)(candset, nchunks)

        # l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr)
        #
        # ltbl = (lproj_df)(ltable, l_proj_attrs)
        # rtbl = (rproj_df)(rtable, r_proj_attrs)

        results = []
        for i in xrange(nchunks):
            result = delayed(self._block_candset_part)(cand_splitted[i], ltable, rtable,
                                                       fk_ltable,
                                                       fk_rtable, l_key, r_key,
                                                       l_block_attr, r_block_attr)
            results.append(result)

        valid_candset = delayed(concat_df)(results)
        if compute:
            valid_candset = exec_dag(valid_candset, num_workers, cache_size, scheduler,
                                     show_progress)
        return valid_candset
