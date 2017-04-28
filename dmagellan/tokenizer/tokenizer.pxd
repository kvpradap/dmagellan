from libcpp.string cimport string
from libcpp.map cimport map as omap
from libcpp.vector cimport vector
from libcpp.set cimport set as oset

cdef class Tokenizer:
    cdef vector[string] ctokenize(self, string s) nogil


