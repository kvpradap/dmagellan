# coding=utf-8

import pandas as pd
from dask import threaded, delayed

from dmagellan.blocker.blocker_utils import get_attrs_to_project, \
    get_rattrs_to_project, get_lattrs_to_project
from dmagellan.utils.py_utils.utils import split_df, proj_df, add_attributes, concat_df, \
    add_id, exec_dag, lsplit_df, rsplit_df, candsplit_df, lproj_df, rproj_df, \
    candproj_df


class BlackBoxBlocker:
    def __init__(self, *args, **kwargs):
        black_box_function = kwargs.get('function', None)
        ltable_attrs = kwargs.get('ltable_attrs', None)
        rtable_attrs = kwargs.get('rtable_attrs', None)
        self.black_box_function = black_box_function
        self.ltable_attrs = ltable_attrs
        self.rtable_attrs = rtable_attrs

    def set_black_box_function(self, function):
        self.black_box_function = function

    def set_ltable_attrs(self, ltable_attrs):
        self.ltable_attrs = ltable_attrs

    def set_rtable_attrs(self, rtable_attrs):
        self.rtable_attrs = rtable_attrs

    def _block_tables_part(self, ltable, rtable, l_key, r_key, l_output_attrs,
                           r_output_attrs, l_output_prefix, r_output_prefix):

        l_proj_attrs = (get_lattrs_to_project)(l_key, l_block_attr, l_output_attrs)
        r_proj_attrs = (get_rattrs_to_project)(r_key, r_block_attr, r_output_attrs)


        l_key_vals = ltable[l_key].values
        r_key_vals = rtable[r_key].values

        ltbl = (lproj_df)(ltable, l_proj_attrs)
        rtbl = (rproj_df)(rtable, r_proj_attrs)

        # if self.ltable_attrs != None:
        #     ltbl = ltable[self.ltable_attrs]
        # else:
        #     ltbl = ltable
        #
        # if self.rtable_attrs != None:
        #     rtbl = rtable[self.rtable_attrs]
        # else:
        #     rtbl = rtable

        l_dict = {}
        r_dict = {}

        for i in xrange(len(ltbl)):
            l_dict[l_key_vals[i]] = ltbl.iloc[i]

        for i in xrange(len(rtbl)):
            r_dict[r_key_vals[i]] = rtbl.iloc[i]

        valid_pairs = []
        for l_id in l_dict:
            l_tuple = l_dict[l_id]
            for r_id in r_dict:
                r_tuple = r_dict[r_id]
                res = self.black_box_function(l_tuple, r_tuple)
                if not res:
                    valid_pairs.append([l_id, r_id])
        fk_ltable, fk_rtable = l_output_prefix + l_key, r_output_prefix + r_key
        candset = pd.DataFrame(valid_pairs, columns=[fk_ltable, fk_rtable])
        candset = add_attributes(candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                                 r_key, l_output_attrs, r_output_attrs,
                                 l_output_prefix, r_output_prefix)
        return candset

    def _block_candset_part(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key,
                            r_key):

        l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs)
        r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs)

        ltbl = (lproj_df)(ltable, l_proj_attrs)
        rtbl = (rproj_df)(rtable, r_proj_attrs)

        ltbl = ltbl.set_index(l_key, drop=False)
        rtbl = rtbl.set_index(r_key, drop=False)
        # if self.ltable_attrs != None:
        #     ltbl = ltbl[self.ltable_attrs]
        #
        # if self.rtable_attrs != None:
        #     rtbl = rtbl[self.rtable_attrs]

        c_df = candset[[fk_ltable, fk_rtable]]
        l_dict = {}
        r_dict = {}
        # list to keep track of valid ids
        valid = []
        l_id_pos = list(c_df.columns).index(fk_ltable)
        r_id_pos = list(c_df.columns).index(fk_rtable)
        for row in c_df.itertuples(index=False):
            row_lkey = row[l_id_pos]
            if row_lkey not in l_dict:
                l_tuple = ltbl.ix[row_lkey]
                l_dict[row_lkey] = l_tuple
            else:
                l_tuple = l_dict[row_lkey]

            # # get rtuple, try dictionary first, then dataframe
            row_rkey = row[r_id_pos]
            if row_rkey not in r_dict:
                r_tuple = rtbl.ix[row_rkey]
                r_dict[row_rkey] = r_tuple
            else:
                r_tuple = r_dict[row_rkey]
            res = self.black_box_function(l_tuple, r_tuple)
            if not res:
                valid.append(True)
            else:
                valid.append(False)
        return candset[valid]

    def block_tables(self, ltable, rtable, l_key, r_key, l_output_attrs=None,
                     r_output_attrs=None, l_output_prefix='l_', r_output_prefix='r_',
                     nltable_chunks=1, nrtable_chunks=1, scheduler=threaded.get,
                     num_workers=None, cache_size=1e9, compute=False, show_progress=True):

        ltable_splitted = (lsplit_df)(ltable, nltable_chunks)
        rtable_splitted = (rsplit_df)(rtable, nrtable_chunks)

        l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs,
                                              l_output_attrs)
        # needs to be modified as self.ltable_attrs can be None.
        r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs,
                                              r_output_attrs)
        results = []
        for i in xrange(nltable_chunks):
            # ltbl = (lproj_df)(ltable_splitted[i], l_proj_attrs)
            for j in xrange(nrtable_chunks):
                # rtbl = (rproj_df)(rtable_splitted[j], r_proj_attrs)
                result = delayed(self._block_tables_part)(ltable_splitted[i],
                                                          rtable_splitted[j],
                                                          l_key, r_key,
                                                   l_output_attrs,
                                                   r_output_attrs, l_output_prefix,
                                                   r_output_prefix)
                results.append(result)
        candset = delayed(concat_df)(results)
        candset = delayed(add_id)(candset)

        if compute:
            candset = exec_dag(candset, num_workers, cache_size, scheduler,
                               show_progress)

        return candset

    def block_candset(self, candset, ltable, rtable, fk_ltable, fk_rtable, l_key, r_key,
                      nchunks=1, scheduler=threaded.get, num_workers=None,
                      cache_size=1e9, compute=False, show_progress=True):
        candset_splitted = delayed(candsplit_df)(candset, nchunks)
        # l_proj_attrs = (get_lattrs_to_project)(l_key, self.ltable_attrs)
        # r_proj_attrs = (get_rattrs_to_project)(r_key, self.rtable_attrs)
        #
        # ltbl = (lproj_df)(ltable, l_proj_attrs)
        # rtbl = (rproj_df)(rtable, r_proj_attrs)

        results = []
        for i in xrange(nchunks):
            result = delayed(self._block_candset_part)(candset_splitted[i], ltable,
                                                       rtable,
                                                       fk_ltable, fk_rtable, l_key, r_key)
            results.append(result)
        valid_candset = delayed(concat_df)(results)

        if compute:
            valid_candset = exec_dag(valid_candset, num_workers, cache_size,
                                     scheduler,
                                     show_progress)
        return valid_candset
