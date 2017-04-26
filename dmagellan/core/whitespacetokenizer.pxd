from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap
from .tokenizer cimport Tokenizer

cdef extern from "string.h":
    char *strtok_r (char *inp_str, const char *delimiters, char **) nogil
cdef class WhiteSpaceTokenizer(Tokenizer):
    cdef omap[string, int] stopwords
    cdef vector[string] cremove_stopwords(self, vector[string]& svec) nogil
    cdef vector[string] ctokenize_wd(self, const string& inp) nogil
