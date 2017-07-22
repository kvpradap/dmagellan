from collections import OrderedDict

import pandas as pd
import numpy as np
from sklearn.model_selection import KFold
from sklearn.utils import indexable
from sklearn.metrics.scorer import check_scoring
from sklearn.base import is_classifier, clone
from sklearn.model_selection._split import check_cv
from sklearn.model_selection._validation import  _fit_and_score

from dask import delayed, threaded


from dmagellan.utils.py_utils.utils import check_attrs_present, list_diff, \
    list_drop_duplicates, exec_dag


# def cross_val_score()


def select_matcher(matchers, x=None, y=None, table=None, exclude_attrs=None,
                   target_attr=None,
                   metric='precision', k=5,
                   random_state=None,
                   scheduler=threaded.get, num_workers=None,
                   cache_size=1e9, compute=False, show_progress=True
                   ):
    x, y = _get_xy_data(x, y, table, exclude_attrs, target_attr)
    scores = []
    for m in matchers:
        score = (cross_validation)(m, x, y, metric, k, random_state)
        scores.append(score)
    res = delayed(process_scores)(matchers, scores, k)
    if compute:
        res = exec_dag(res, num_workers, cache_size,
                             scheduler,
                             show_progress)
    return res

def process_scores(matchers, scores_list, k):
    dict_list = []
    max_score = 0
    # Initialize the best matcher. As of now set it to be the first matcher.
    sel_matcher = matchers[0]
    # Fix the header
    header = ['Name', 'Matcher', 'Num folds']
    # # Append the folds
    fold_header = ['Fold ' + str(i + 1) for i in range(k)]
    header.extend(fold_header)
    # Finally, append the score.
    header.append('Mean score')
    for i in range(len(matchers)):
        matcher, scores = matchers[i], scores_list[i]
        val_list = [matcher.get_name(), matcher, k]
        val_list.extend(scores)
        val_list.append(pd.np.mean(scores))
        d = OrderedDict(zip(header, val_list))
        dict_list.append(d)
        if pd.np.mean(scores) > max_score:
            sel_matcher = matcher
            max_score = pd.np.mean(scores)
    stats = pd.DataFrame(dict_list)
    stats = stats[header]
    res = OrderedDict()
    # Add selected matcher and the stats to a dictionary.
    res['selected_matcher'] = sel_matcher
    res['cv_stats'] = stats
    # Result the final dictionary containing selected matcher and the CV
    # statistics.
    return res


def concat_cv_scores(scores):
    return np.array(scores)[:, 0]

def cross_val_score(estimator, X, y=None, groups=None, scoring=None, cv=None,
                    fit_params=None, verbose=0):
    X, y, groups = indexable(X, y, groups)
    cv = check_cv(cv, y, classifier=is_classifier(estimator))
    scorer = check_scoring(estimator, scoring=scoring)
    scores = []
    for train, test in cv.split(X, y, groups):
        score = delayed(_fit_and_score)(clone(estimator), X, y, scorer, train, test, \
                                 verbose, None, fit_params)
        scores.append(score)
    result = delayed(concat_cv_scores)(scores)
    return result




# this is going to return a delayed object.
def cross_validation(matcher, x, y, metric, k, random_state):
    """
    The function does cross validation for a single matcher
    """
    # Use KFold function from scikit learn to create a ms object that can be
    # used for cross_val_score function.
    cv = KFold(k, shuffle=True, random_state=random_state)
    # Call the scikit-learn's cross_val_score function
    scores = cross_val_score(matcher.clf, x, y, scoring=metric, cv=cv)
    # Finally, return the scores
    return scores


def _get_xy_data(x, y, table, exclude_attrs, target_attr):
    """
    Gets the X, Y data from the input based on the given table, the
    exclude attributes and target attribute provided.
    """
    # If x and y is given, call appropriate function
    if x is not None and y is not None:
        return _get_xy_data_prj(x, y)
    # If table, exclude attributes and target attribute are given,
    # call appropriate function
    elif table is not None and exclude_attrs is not None \
            and target_attr is not None:
        return _get_xy_data_ex(table, exclude_attrs, target_attr)
    else:
        # Else, raise a syntax error.
        raise SyntaxError('The arguments supplied does not match '
                          'the signatures supported !!!')


def _get_xy_data_prj(x, y):
    """
    Gets X, Y that can be used for scikit-learn function based on given
    projected tables.
    """
    # If the first column is '_id' the remove it
    if x.columns[0] == '_id':
        logger.warning(
            'Input table contains "_id". Removing this column for processing')
        # Get the values  from the DataFrame
        x = x.values
        x = pd.np.delete(x, 0, 1)
    else:
        x = x.values

    if not isinstance(y, pd.Series):
        # logger.error('Target attr is expected to be a pandas series')
        raise AssertionError('Target attr is expected to be a pandas series')
    else:
        # Get the values  from the DataFrame
        y = y.values
    # Finally, return x an y
    return x, y


def _get_xy_data_ex(table, exclude_attrs, target_attr):
    # Validate the input parameters
    # # We expect the input table to be of type pandas DataFrame
    if not isinstance(table, pd.DataFrame):
        # logger.error('Input table is not of type DataFrame')
        raise AssertionError(
            logger.error('Input table is not of type dataframe'))

    # We expect exclude attributes to be of type list. If not convert it into
    #  a list.
    if not isinstance(exclude_attrs, list):
        exclude_attrs = [exclude_attrs]

    # Check if the exclude attributes are present in the input table
    if not check_attrs_present(table, exclude_attrs):
        # logger.error('The attributes mentioned in exclude_attrs '
        #              'is not present '
        #              'in the input table')
        raise AssertionError(
            'The attributes mentioned in exclude_attrs '
            'is not present '
            'in the input table')
    # Check if the target attribute is present in the input table
    if not check_attrs_present(table, target_attr):
        # logger.error('The target_attr is not present in the input table')
        raise AssertionError(
            'The target_attr is not present in the input table')

    # Drop the duplicates from the exclude attributes
    exclude_attrs = list_drop_duplicates(exclude_attrs)

    # Explicitly add the target attribute to exclude attribute (if it is not
    # already present)
    if target_attr not in exclude_attrs:
        exclude_attrs.append(target_attr)

    # Project the list of attributes that should be used for scikit-learn's
    # functions.
    attrs_to_project = list_diff(list(table.columns), exclude_attrs)

    # Get the values for x
    x = table[attrs_to_project].values
    # Get the values for x
    y = table[target_attr].values
    y = y.ravel()  # to mute warnings from svm and cross validation
    # Return x and y
    return x, y
