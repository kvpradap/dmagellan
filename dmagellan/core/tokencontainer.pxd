from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap

from .stringcontainer cimport StringContainer

cdef extern from "string.h" nogil:
        char *strtok_r (char *inp_str, const char *delimiters, char **)

cdef class TokenContainer:
    cdef vector[pair[int, vector[string]]] container

    cdef int _get_size(self) nogil
    cdef vector[pair[int,vector[string]]] _get(self)
    cdef pair[int, vector[string]] _get_index(self, int i) nogil
    cdef vector[string] _stokenize(self, const string& inp_string) nogil
    cdef vector[string] _remove_stopwords(self, vector[string] &inp_tokens,
                                    const omap[string, int] &stop_words) nogil

    cdef void _tokenize(self, StringContainer container, omap[string, int] stopwords) nogil
