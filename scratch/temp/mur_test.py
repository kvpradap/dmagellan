import py_entitymatching as em
import os
import sys
################## helper ################
def _get_stop_words():
    stop_words_set = set()
    install_path = em.get_install_path()
    dataset_path = os.sep.join([install_path, 'utils'])
    stop_words_file = os.sep.join([dataset_path, 'stop_words.txt'])
    with open(stop_words_file, "rb") as stopwords_file:
        for stop_words in stopwords_file:
            stop_words_set.add(stop_words.rstrip())

    return stop_words_set
########################################



# sys.path.append('/scratch/pradap/python-work/dmagellan')
sys.path.append('/users/pradap/Documents/Research/Python-Package/scaling/dmagellan')
from dmagellan.core.downsample import downsample_sm, downsample_dk, downsample_dbg


stopwords = list(_get_stop_words())
stopwords.extend(['the', 'my', 'i', 'andre', 'from', 'a', 'of', 'the', 'version', 'love', 'live', 'la', 'mix', 'album', 'dont'])


A = em.load_dataset('person_table_A')
B = em.load_dataset('person_table_B')

# res_sm = downsample_sm(A, B, 3, 1, stopwords=stopwords)
#
# print(res_sm[0])
# print(res_sm[1])
from dask.async import get_sync

res_dk = downsample_dk(A, B, 3, 1, stopwords=stopwords, nchunks=3)
print(res_dk[0])
print(res_dk[1])





