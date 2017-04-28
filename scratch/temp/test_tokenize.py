import sys
sys.path.append('/Users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
from test_preprocess import preprocess_table
from dmagellan.TEMP.tokencontainer import TokenContainer
from dmagellan.TEMP.invertedindex import InvertedIndex


def tokenize_strings(concat_strings, stopwords):
    n = concat_strings.get_size()
    tok_container_obj = TokenContainer()
    tok_container_obj.tokenize(concat_strings, stopwords)
    return tok_container_obj

def build_inv_index(tokens):
    inv_obj = InvertedIndex()
    inv_obj.build_inv_index(tokens)
    return inv_obj

import py_entitymatching as em
A = em.load_dataset('person_table_A')
concat_strings = preprocess_table(A)
tokens = tokenize_strings(concat_strings, ['san'])
print(tokens.get_index(0))
invindex = build_inv_index(tokens)
print(invindex.get_values('franciscoo'))
import sys
print(sys.getsizeof(concat_strings))
