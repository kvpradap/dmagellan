import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.core.tokencontainer import TokenContainer
from dmagellan.core.stringcontainer import StringContainer
from dmagellan.core.invertedindex import InvertedIndex
from dmagellan.core.downsample import downsample

import pandas as pd


A = pd.read_csv('../datasets/tracks.csv')
B = pd.read_csv('../datasets/songs.csv')

(a, b) = downsample(A, B, 10000, 1, stopwords=['the'])
