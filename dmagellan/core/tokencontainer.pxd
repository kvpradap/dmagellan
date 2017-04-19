from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as omap
from libcpp.set cimport set as oset


cdef extern from "string.h":
    char *strtok_r (char *inp_str, const char *delimiters, char **) nogil

cdef class TokenContainer:
    cdef vector[vector[string]] box
    cdef int csize(self)
    cdef void cinit(self, int n) nogil
    cdef void cpush_back(self, vector[string] tokens)
    cdef vector[string] cremove_stopwords(self, vector[string]& svec, omap[string, int]& stopwords) nogil
    cdef vector[string] ctokenize_wd(self, const string& inp) nogil
    cdef void ctokenize(self, vector[string] &svec, omap[string, int]& stopwords) nogil
