"""
This module contains functions to extract features using a feature table.
THIS FUNCTION NEEDS TO BE MODIFIED TO GET RID OF CATALOG HELPER
"""
import logging

import pandas as pd

from dmagellan.utils.py_utils.utils import str2bytes, split_df, build_inv_index, \
    proj_df, tokenize_strings, add_attributes, concat_df, add_id, exec_dag, lsplit_df, \
    rsplit_df, candsplit_df, lproj_df, rproj_df, candproj_df, list_diff, list_drop_duplicates

from dask import get, delayed

logger = logging.getLogger(__name__)


def _extract_feature_vecs_part(candset, ltable, rtable, key, fk_ltable, fk_rtable, l_key,
                               r_key, attrs_before, feature_table, attrs_after):
    # ltable = ltable.compute()
    # rtable = rtable.compute()
    if isinstance(candset, pd.DataFrame) and len(candset):
        ltbl = ltable.set_index(l_key, drop=False)
        rtbl = rtable.set_index(r_key, drop=False)
        c_df = candset[[fk_ltable, fk_rtable]]

        l_dict = {}
        r_dict = {}

        l_id_pos = list(c_df.columns).index(fk_ltable)
        r_id_pos = list(c_df.columns).index(fk_rtable)
        feat_vals = []
        for row in c_df.itertuples(index=False):
            row_lkey = row[l_id_pos]
            if row_lkey not in l_dict:
                l_tuple = ltbl.ix[row_lkey]
                l_dict[row_lkey] = l_tuple
            else:
                l_tuple = l_dict[row_lkey]
            row_rkey = row[r_id_pos]
            if row_rkey not in r_dict:
                r_tuple = rtbl.ix[row_rkey]
                r_dict[row_rkey] = r_tuple
            else:
                r_tuple = r_dict[row_rkey]

            f = apply_feat_fns(l_tuple, r_tuple, feature_table)
            feat_vals.append(f)
        feature_vectors = pd.DataFrame(feat_vals, index=candset.index.values)
        # # Rearrange the feature names in the input feature table order
        feature_names = list(feature_table['feature_name'])
        feature_vectors = feature_vectors[feature_names]
        if attrs_before:
            if not isinstance(attrs_before, list):
                attrs_before = [attrs_before]
            attrs_before = gh.list_diff(attrs_before, [key, fk_ltable, fk_rtable])
            attrs_before.reverse()
            for a in attrs_before:
                feature_vectors.insert(0, a, candset[a])

        # # Insert keys
        feature_vectors.insert(0, fk_rtable, candset[fk_rtable])
        feature_vectors.insert(0, fk_ltable, candset[fk_ltable])
        feature_vectors.insert(0, key, candset[key])
        # # insert attrs after
        if attrs_after:
            if not isinstance(attrs_after, list):
                attrs_after = [attrs_after]
            attrs_after = list_diff(attrs_after, [key, fk_ltable, fk_rtable])
            attrs_after.reverse()
            col_pos = len(feature_vectors.columns)
            for a in attrs_after:
                feature_vectors.insert(col_pos, a, candset[a])
                col_pos += 1

        # Finally, return the feature vectors
        return feature_vectors
    else:
        return None


def extract_feature_vecs(candset, ltable, rtable, key, fk_ltable, fk_rtable, l_key,
                         r_key, attrs_before=None,
                         feature_table=None,
                         attrs_after=None,
                         nchunks=1, scheduler=get, num_workers=None,
                         cache_size=1e9, compute=False, show_progress=True):
    candset_splitted = delayed(candsplit_df)(candset, nchunks)
    results = []
    for i in xrange(nchunks):
        result = delayed(_extract_feature_vecs_part)(candset_splitted[i], ltable, rtable,
                                                     key,
                                                     fk_ltable, fk_rtable, l_key, r_key,
                                                     attrs_before, feature_table,
                                                     attrs_after)
        results.append(result)
    feat_vecs = delayed(concat_df)(results)

    if compute:
        feat_vecs = exec_dag(feat_vecs, num_workers, cache_size,
                             scheduler,
                             show_progress)
    return feat_vecs



def apply_feat_fns(tuple1, tuple2, feat_dict):
    """
    Apply feature functions to two tuples.
    """
    # Get the feature names
    feat_names = list(feat_dict['feature_name'])
    # Get the feature functions
    feat_funcs = list(feat_dict['function'])
    # Compute the feature value by applying the feature function to the input
    #  tuples.
    feat_vals = [f(tuple1, tuple2) for f in feat_funcs]
    # Return a dictionary where the keys are the feature names and the values
    #  are the feature values.
    return dict(zip(feat_names, feat_vals))
