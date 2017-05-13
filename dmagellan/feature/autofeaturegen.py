"""
This module contains functions for auto feature generation.
"""
import logging

import pandas as pd
import six

import dmagellan.feature.attributeutils as au
import dmagellan.feature.simfunctions as sim
import dmagellan.feature.tokenizers as tok

logger = logging.getLogger(__name__)


def get_features(ltable, rtable, l_attr_types, r_attr_types,
                 attr_corres, tok_funcs, sim_funcs):

    # Validate input parameters
    # # We expect the ltable to be of type pandas DataFrame
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas DataFrame')
        raise AssertionError('Input ltable is not of type pandas DataFrame')

    # # We expect the rtable to be of type pandas DataFrame
    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas DataFrame')
        raise AssertionError('Input rtable is not of type pandas DataFrame')

    # # We expect the l_attr_types to be of type python dictionary
    if not isinstance(l_attr_types, dict):
        logger.error('Input l_attr_types is not of type dictionary')
        raise AssertionError('Input l_attr_types is not of type dictionary')

    # # We expect the r_attr_types to be of type python dictionary
    if not isinstance(r_attr_types, dict):
        logger.error('Input r_attr_types is not of type dictionary')
        raise AssertionError('Input r_attr_types is not of type dictionary')

    # # We expect the attr_corres to be of type python dictionary
    if not isinstance(attr_corres, dict):
        logger.error('Input attr_corres is not of type dictionary')
        raise AssertionError('Input attr_corres is not of type dictionary')

    # # We expect the tok_funcs to be of type python dictionary
    if not isinstance(tok_funcs, dict):
        logger.error('Input tok_funcs is not of type dictionary')
        raise AssertionError('Input tok_funcs is not of type dictionary')

    # # We expect the sim_funcs to be of type python dictionary
    if not isinstance(sim_funcs, dict):
        logger.error('Input sim_funcs is not of type dictionary')
        raise AssertionError('Input sim_funcs is not of type dictionary')

    # We expect the table order to be same in l/r_attr_types and attr_corres
    if not _check_table_order(ltable, rtable,
                              l_attr_types, r_attr_types, attr_corres):
        raise AssertionError('Table order is different than what is mentioned '
                             'in l/r attr_types and attr_corres')

    # Initialize output feature dictionary list
    feature_dict_list = []

    # Generate features for each attr. correspondence
    for ac in attr_corres['corres']:
        l_attr_type = l_attr_types[ac[0]]
        r_attr_type = r_attr_types[ac[1]]

        # Generate a feature only if the attribute types are same
        if l_attr_type != r_attr_type:
            logger.info('py_entitymatching types: %s type (%s) and %s type (%s) '
                           'are different.'
                           'If you want to set them to be same and '
                           'generate features, '
                           'update output from get_attr_types and '
                           'use get_features command.\n.'
                           % (ac[0], l_attr_type, ac[1], r_attr_type))
            # features_1 = _get_features_for_type(l_attr_type)
            # features_2 = _get_features_for_type(r_attr_type)
            # features = set(features_1).union(features_2)
            continue

        # Generate features
        features = _get_features_for_type(l_attr_type)

        # Convert features to function objects
        fn_objs = _conv_func_objs(features, ac, tok_funcs, sim_funcs)
        # Add the function object to a feature list.
        feature_dict_list.append(fn_objs)

    # Create a feature table
    feature_table = pd.DataFrame(flatten_list(feature_dict_list))
    # Project out only the necessary columns.
    feature_table = feature_table[['feature_name', 'left_attribute',
                                   'right_attribute', 'left_attr_tokenizer',
                                   'right_attr_tokenizer',
                                   'simfunction', 'function',
                                   'function_source', 'is_auto_generated']]
    # Return the feature table.
    return feature_table


def get_features_for_blocking(ltable, rtable):
    """

    This function automatically generates features that can be used for
    blocking purposes.

    Args:
        ltable,rtable (DataFrame): The pandas DataFrames for which the
            features are to be generated.

    Returns:
        A pandas DataFrame containing automatically generated features.

        Specifically, the DataFrame contains the following attributes:
        'feature_name', 'left_attribute', 'right_attribute',
        'left_attr_tokenizer', 'right_attr_tokenizer', 'simfunction',
        'function', 'function_source', and 'is_auto_generated'.


        Further, this function also sets the following global variables:
        _block_t, _block_s, _atypes1, _atypes2, and _block_c.

        The variable _block_t contains the tokenizers used and  _block_s
        contains the similarity functions used for creating features.

        The variables _atypes1, and  _atypes2 contain the attribute types for
        ltable and rtable respectively. The variable _block_c contains the
        attribute correspondences between the two input tables.

    Raises:
        AssertionError: If `ltable` is not of type pandas
            DataFrame.
        AssertionError: If `rtable` is not of type pandas
            DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> block_f = em.get_features_for_blocking(A, B)

    Note:
        In the output DataFrame, two
        attributes demand some explanation: (1) function, and (2)
        is_auto_generated. The function, points to the actual Python function
        that implements the feature. Specifically, the function takes in two
        tuples (one from each input table) and returns a numeric value. The
        attribute is_auto_generated contains either True or False. The flag
        is True only if the feature is automatically generated by py_entitymatching.
        This is important because this flag is used to make some assumptions
        about the semantics of the similarity function used and use that
        information for scaling purposes.

    See Also:
     :meth:`py_entitymatching.get_attr_corres`, :meth:`py_entitymatching.get_attr_types`,
     :meth:`py_entitymatching.get_sim_funs_for_blocking`
     :meth:`py_entitymatching.get_tokenizers_for_blocking`

    """
    # Validate input parameters
    # # We expect the ltable to be of type pandas DataFrame
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input table A is not of type pandas DataFrame')
        raise AssertionError('Input table A is not of type pandas DataFrame')

    # # We expect the rtable to be of type pandas DataFrame
    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input table B is not of type pandas dataframe')
        raise AssertionError('Input table B is not of type pandas dataframe')
    # Get the similarity functions to be used for blocking
    sim_funcs = sim.get_sim_funs_for_blocking()
    # Get the tokenizers to be used for blocking
    tok_funcs = tok.get_tokenizers_for_blocking()

    # Get the attr. types for ltable and rtable
    attr_types_ltable = au.get_attr_types(ltable)
    attr_types_rtable = au.get_attr_types(rtable)
    # Get the attr. correspondences between ltable and rtable
    attr_corres = au.get_attr_corres(ltable, rtable)
    # Get features based on attr types, attr correspondences, sim functions
    # and tok. functions
    feature_table = get_features(ltable, rtable, attr_types_ltable,
                                 attr_types_rtable, attr_corres,
                                 tok_funcs, sim_funcs)

    # Export important variables to global name space
    # em._match_t = tok_funcs
    # em._block_s = sim_funcs
    # em._atypes1 = attr_types_ltable
    # em._atypes2 = attr_types_rtable
    # em._block_c = attr_corres
    # Return the feature table
    return feature_table


def get_features_for_matching(ltable, rtable):
    """
    This function automatically generates features that can be used for
    matching purposes.

    Args:
        ltable,rtable (DataFrame): The pandas DataFrames for which the
            features are to be generated.

    Returns:
        A pandas DataFrame containing automatically generated features.

        Specifically, the DataFrame contains the following attributes:
        'feature_name', 'left_attribute', 'right_attribute',
        'left_attr_tokenizer', 'right_attr_tokenizer', 'simfunction',
        'function', 'function_source', and 'is_auto_generated'.


        Further, this function also sets the following global variables:
        _match_t, _match_s, _atypes1, _atypes2, and _match_c.

        The variable _match_t contains the tokenizers used and  _match_s
        contains the similarity functions used for creating features.

        The variables _atypes1, and  _atypes2 contain the attribute types for
        ltable and rtable respectively. The variable _match_c contains the
        attribute correspondences between the two input tables.

    Raises:
        AssertionError: If `ltable` is not of type pandas
            DataFrame.
        AssertionError: If `rtable` is not of type pandas
            DataFrame.

    Examples:
        >>> import py_entitymatching as em
        >>> A = em.read_csv_metadata('path_to_csv_dir/table_A.csv', key='ID')
        >>> B = em.read_csv_metadata('path_to_csv_dir/table_B.csv', key='ID')
        >>> match_f = em.get_features_for_matching(A, B)

    Note:
        In the output DataFrame, two
        attributes demand some explanation: (1) function, and (2)
        is_auto_generated. The function, points to the actual Python function
        that implements the feature. Specifically, the function takes in two
        tuples (one from each input table) and returns a numeric value. The
        attribute is_auto_generated contains either True or False. The flag
        is True only if the feature is automatically generated by py_entitymatching.
        This is important because this flag is used to make some assumptions
        about the semantics of the similarity function used and use that
        information for scaling purposes.

    See Also:
     :meth:`py_entitymatching.get_attr_corres`, :meth:`py_entitymatching.get_attr_types`,
     :meth:`py_entitymatching.get_sim_funs_for_matching`
     :meth:`py_entitymatching.get_tokenizers_for_matching`

    """
    # Validate input parameters
    # # We expect the ltable to be of type pandas DataFrame
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input table A is not of type pandas DataFrame')
        raise AssertionError('Input table A is not of type pandas DataFrame')

    # # We expect the rtable to be of type pandas DataFrame
    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input table B is not of type pandas DataFrame')
        raise AssertionError('Input table B is not of type pandas DataFrame')

    # Get similarity functions for generating the features for matching
    sim_funcs = sim.get_sim_funs_for_matching()
    # Get tokenizer functions for generating the features for matching
    tok_funcs = tok.get_tokenizers_for_matching()

    # Get the attribute types of the input tables
    attr_types_ltable = au.get_attr_types(ltable)
    attr_types_rtable = au.get_attr_types(rtable)

    # Get the attribute correspondence between the input tables
    attr_corres = au.get_attr_corres(ltable, rtable)

    # Get the features
    feature_table = get_features(ltable, rtable, attr_types_ltable,
                                 attr_types_rtable, attr_corres,
                                 tok_funcs, sim_funcs)

    # Export important variables to global name space
    # em._match_t = tok_funcs
    # em._match_s = sim_funcs
    # em._atypes1 = attr_types_ltable
    # em._atypes2 = attr_types_rtable
    # em._match_c = attr_corres

    # Finally return the feature table
    return feature_table


#
def _check_table_order(ltable, rtable, l_attr_types, r_attr_types, attr_corres):
    """
    Check whether the order of tables matches with what is mentioned in
    l_attr_types, r_attr_type and attr_corres.
    """
    # Validate the input parameters
    # We expect the input object ltable to be of type pandas DataFrame
    if not isinstance(ltable, pd.DataFrame):
        logger.error('Input ltable is not of type pandas dataframe')
        raise AssertionError('Input ltable is not of type pandas dataframe')

    # We expect the input object rtable to be of type pandas DataFrame
    if not isinstance(rtable, pd.DataFrame):
        logger.error('Input rtable is not of type pandas dataframe')
        raise AssertionError('Input rtable is not of type pandas dataframe')

    # Get the ids of the input tables. This is used to validate the order
    # of tables present in the given data structures.
    # Note: This kind of checking is bit too aggressive, the reason is this
    # checking needs the ltable and rtable to point to exact memory location
    # across the given dictionaries and the input. Ideally, we just need to
    # check whether the contents of those DataFrames are same.
    ltable_id = id(ltable)
    rtable_id = id(rtable)

    # Check whether ltable id matches with id of table mentioned in l_attr_types
    if ltable_id != id(l_attr_types['_table']):
        logger.error(
            'ltable is not the same as table mentioned in left attr types')
        return False

    # Check whether rtable id matches with id of table mentioned in r_attr_types
    if rtable_id != id(r_attr_types['_table']):
        logger.error(
            'rtable is not the same as table mentioned in right attr types')
        return False

    # Check whether ltable matches with ltable mentioned in attr_corres
    if ltable_id != id(attr_corres['ltable']):
        logger.error(
            'ltable is not the same as table mentioned in attr correspondence')
        return False

    # Check whether rtable matches with rtable mentioned in attr_corres
    if rtable_id != id(attr_corres['rtable']):
        logger.error(
            'rtable is not the same as table mentioned in attr correspondence')
        return False

    # Finally, return True.
    return True


# get look up table to generate features
def _get_feat_lkp_tbl():
    """
    This function embeds the knowledge of mapping what features to be
    generated for what kind of attr. types.

    """
    # Initialize a lookup table
    lookup_table = dict()

    # Features for type str_eq_1w
    lookup_table['STR_EQ_1W'] = [('lev_dist'), ('lev_sim'), ('jaro'),
                                ('jaro_winkler'),
                                 ('exact_match'),
                                 ('jaccard', 'qgm_3', 'qgm_3')]

    # Features for type str_bt_1w_5w
    lookup_table['STR_BT_1W_5W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                                    ('cosine', 'dlm_dc0', 'dlm_dc0'),
                                    ('jaccard', 'dlm_dc0', 'dlm_dc0'),
                                    ('monge_elkan'), ('lev_dist'), ('lev_sim'),
                                    ('needleman_wunsch'),
                                    ('smith_waterman')]  # dlm_dc0 is the concrete space tokenizer

    # Features for type str_bt_5w_10w
    lookup_table['STR_BT_5W_10W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                                     ('cosine', 'dlm_dc0', 'dlm_dc0'),
                                     ('monge_elkan'), ('lev_dist'), ('lev_sim')]

    # Features for type str_gt_10w
    lookup_table['STR_GT_10W'] = [('jaccard', 'qgm_3', 'qgm_3'),
                                  ('cosine', 'dlm_dc0', 'dlm_dc0')]

    # Features for NUMERIC type
    lookup_table['NUM'] = [('exact_match'), ('abs_norm'), ('lev_dist'),
                           ('lev_sim')]

    # Features for BOOLEAN type
    lookup_table['BOOL'] = [('exact_match')]

    # Features for un determined type
    lookup_table['UN_DETERMINED'] = []

    # Finally, return the lookup table
    return lookup_table


def _get_features_for_type(column_type):
    """
    Get features to be generated for a type
    """
    # First get the look up table
    lookup_table = _get_feat_lkp_tbl()

    # Based on the column type, return the feature functions that should be
    # generated.
    if column_type is 'str_eq_1w':
        features = lookup_table['STR_EQ_1W']
    elif column_type is 'str_bt_1w_5w':
        features = lookup_table['STR_BT_1W_5W']
    elif column_type is 'str_bt_5w_10w':
        features = lookup_table['STR_BT_5W_10W']
    elif column_type is 'str_gt_10w':
        features = lookup_table['STR_GT_10W']
    elif column_type is 'numeric':
        features = lookup_table['NUM']
    elif column_type is 'boolean':
        features = lookup_table['BOOL']
    elif column_type is 'un_determined':
        features = lookup_table['UN_DETERMINED']
    else:
        raise TypeError('Unknown type')
    return features


def get_magellan_str_types():
    """
    This function returns the py_entitymatching types as a list of  strings.
    """

    return ['str_eq_1w', 'str_bt_1w_5w', 'str_bt_5w_10w', 'str_gt_10w',
            'numeric', 'boolean', 'un_determined']


# convert features from look up table to function objects
def _conv_func_objs(features, attributes,
                    tokenizer_functions, similarity_functions):
    """
    Convert features from look up table to function objects
    """
    # We need to check whether the features have allowed tokenizers and
    # similarity functions.

    # # First get the tokenizer and similarity functions list.
    tokenizer_list = tokenizer_functions.keys()
    similarity_functions_list = similarity_functions.keys()

    # # Second get the features that uses only valid tokenizers and
    # similarity functions
    valid_list = [check_valid_tok_sim(feature, tokenizer_list,
                                      similarity_functions_list)
                  for feature in features]

    # Get function as a string and other meta data; finally we will get a
    # list of tuples
    function_tuples = [get_fn_str(input, attributes) for input in valid_list]

    # Convert the function string into a function object
    function_objects = conv_fn_str_to_obj(function_tuples, tokenizer_functions,
                                   similarity_functions)

    return function_objects


# check whether tokenizers and simfunctions are allowed
# inp is of the form ('jaccard', 'qgm_3', 'qgm_3') or ('lev')
def check_valid_tok_sim(inp, simlist, toklist):
    if isinstance(inp, six.string_types):
        inp = [inp]
    assert len(inp) == 1 or len(
        inp) == 3, 'len of feature config should be 1 or 3'
    # check whether the sim function in features is in simlist
    if len(set(inp).intersection(simlist)) > 0:
        return inp
    # check whether the tokenizer in features is in tok list
    if len(set(inp).intersection(toklist)) > 0:
        return inp
    return None


# get function string for a feature
def get_fn_str(inp, attrs):
    if inp:
        args = []
        args.extend(attrs)
        if isinstance(inp, six.string_types) == True:
            inp = [inp]
        args.extend(inp)
        # fill function string from a template
        return fill_fn_template(*args)
    else:
        return None


# fill function template
def fill_fn_template(attr1, attr2, sim_func, tok_func_1=None, tok_func_2=None):
    # construct function string
    s = 'from dmagellan.feature.simfunctions import *\nfrom ' \
        'dmagellan.feature.tokenizers import *\n'
    # get the function name
    fn_name = get_fn_name(attr1, attr2, sim_func, tok_func_1, tok_func_2)
    # proceed with function construction
    fn_st = 'def ' + fn_name + '(ltuple, rtuple):'
    s += fn_st
    s += '\n'

    # add 4 spaces
    s += '    '
    fn_body = 'return '
    if tok_func_1 is not None and tok_func_2 is not None:
        fn_body = fn_body + sim_func + '(' + tok_func_1 + '(' + 'ltuple["' + attr1 + '"]'
        fn_body += '), '
        fn_body = fn_body + tok_func_2 + '(' + 'rtuple["' + attr2 + '"]'
        fn_body = fn_body + ')) '
    else:
        fn_body = fn_body + sim_func + '(' + 'ltuple["' + attr1 + '"], rtuple["' + attr2 + '"])'
    s += fn_body

    return fn_name, attr1, attr2, tok_func_1, tok_func_2, sim_func, s


# construct function name from attrs, tokenizers and sim funcs

# sim_fn_names=['jaccard', 'lev', 'cosine', 'monge_elkan',
#               'needleman_wunsch', 'smith_waterman', 'jaro', 'jaro_winkler',
#               'exact_match', 'rel_diff', 'abs_norm']
def get_fn_name(attr1, attr2, sim_func, tok_func_1=None, tok_func_2=None):
    attr1 = '_'.join(attr1.split())
    attr2 = '_'.join(attr2.split())
    fp = '_'.join([attr1, attr2])
    name_lkp = dict()
    name_lkp["jaccard"] = "jac"
    name_lkp["lev_dist"] = "lev_dist"
    name_lkp["lev_sim"] = "lev_sim"
    name_lkp["cosine"] = "cos"
    name_lkp["monge_elkan"] = "mel"
    name_lkp["needleman_wunsch"] = "nmw"
    name_lkp["smith_waterman"] = "sw"
    name_lkp["jaro"] = "jar"
    name_lkp["jaro_winkler"] = "jwn"
    name_lkp["exact_match"] = "exm"
    name_lkp["abs_norm"] = "anm"
    name_lkp["rel_diff"] = "rdf"
    name_lkp["1"] = "1"
    name_lkp["2"] = "2"
    name_lkp["3"] = "3"
    name_lkp["4"] = "4"
    name_lkp["tok_whitespace"] = "wsp"
    name_lkp["tok_qgram"] = "qgm"
    name_lkp["tok_delim"] = "dlm"

    arg_list = [sim_func, tok_func_1, tok_func_2]
    nm_list = [name_lkp.get(tok, tok) for tok in arg_list if tok]
    sp = '_'.join(nm_list)
    return '_'.join([fp, sp])


# conv function string to function object and return with meta data
def conv_fn_str_to_obj(fn_tup, tok, sim_funcs):
    d_orig = {}
    d_orig.update(tok)
    d_orig.update(sim_funcs)
    d_ret_list = []
    for f in fn_tup:
        d_ret = {}
        name = f[0]
        attr1 = f[1]
        attr2 = f[2]
        tok_1 = f[3]
        tok_2 = f[4]
        simfunction = f[5]
        # exec(f[6] in d_orig)
        six.exec_(f[6], d_orig)
        d_ret['function'] = d_orig[name]
        d_ret['feature_name'] = name
        d_ret['left_attribute'] = attr1
        d_ret['right_attribute'] = attr2
        d_ret['left_attr_tokenizer'] = tok_1
        d_ret['right_attr_tokenizer'] = tok_2
        d_ret['simfunction'] = simfunction
        d_ret['function_source'] = f[6]
        d_ret['is_auto_generated'] = True

        d_ret_list.append(d_ret)
    return d_ret_list


def flatten_list(inp_list):
    return [item for sublist in inp_list for item in sublist]
