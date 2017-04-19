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



sys.path.append('/scratch/pradap/python-work/dmagellan')
from dmagellan.core.downsample import downsample_sm


stopwords = list(_get_stop_words())
stopwords.extend(['the', 'my', 'i', 'andre', 'from', 'a', 'of', 'the', 'version', 'love', 'live', 'la', 'mix', 'album', 'dont'])


A = em.load_dataset('person_table_A')
B = em.load_dataset('person_table_B')

res = downsample_sm(A, B, 3, 1, stopwords=stopwords)

print(res[0])
print(res[1])


