from dmagellan.matcher.mlmatcher import MLMatcher
from dmagellan.utils.py_utils.utils import get_ts

from sklearn.linear_model import LogisticRegression


class LogRegMatcher(MLMatcher):
    """
    Logistic Regression matcher.

    Args:
        *args,**kwargs: THe Arguments to scikit-learn's Logistic Regression
            classifier.
        name (string): The name of this matcher (defaults to None). If the
            matcher name is None, the class automatically generates a string
            and assigns it as the name.


    """
    def __init__(self, *args, **kwargs):
        # If the name is given, then pop it
        name = kwargs.pop('name', None)
        if name is None:
            # If the name of the matcher is give, then create one.
            # Currently, we use a constant string + a random number.
            self.name = 'LogisticRegression'+ '_' + get_ts()
        else:
            # Set the name of the matcher, with the given name.
            self.name = name
        super(LogRegMatcher, self).__init__()
        # Set the classifier to the scikit-learn classifier.
        self.clf = LogisticRegression(*args, **kwargs)