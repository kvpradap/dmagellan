import os
import sys

sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
#sys.path.append('/scratch/pradap/python-work/dmagellan')
#from test_preprocess import preprocess_table
from dmagellan.blocker.overlap.overlapblocker import *
from dmagellan.tokenizer.whitespacetokenizer import WhiteSpaceTokenizer

import pandas as pd

datapath = "../datasets/"
A = pd.read_csv(os.path.join(datapath, 'person_table_A.csv'), low_memory=False)
B = pd.read_csv(os.path.join(datapath, 'person_table_B.csv'), low_memory=False)
print('Reading the files done')
tokenizer = WhiteSpaceTokenizer()
# def block_table_part(ldf, rdf, l_key, r_key, l_attr, r_attr, tokenizer, threshold,  stopwords, l_out, r_out, l_prefix, r_prefix):
C = block_tables_sm(A, B, 'ID', 'ID', 'address', 'address',
                    tokenizer, 2,  l_out=['zipcode', 'name', 'address'], r_out=['zipcode', 'name', 'address'],
                    stopwords=['san', 'st'], nlchunks=2, nrchunks=2)
print(len(C))
# print(C)

D = block_candset_sm(C, A, B, "l_ID", "r_ID", "ID", "ID", "name", "name",  tokenizer, 1, nchunks=1)
print(len(D))
# # print(len(D))
# print('\nsingle machine')
# start = time.time()
# C = block_tables_sm(A, B, 'id', 'id', 'title', 'title', ['year'], ['year'], nlchunks=1,
#                     nrchunks=2)
# print('Overlap blocker (sm): {0}'.format(time.time()-start))
# print(len(C))
