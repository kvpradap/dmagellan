from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.map cimport map as omap

cdef class InvertedIndex:
    cdef omap[string, vector[int]] index
    cdef vector[int] cvalues(self, string token)
    cdef void cbuild_inv_index(self, vector[int]&, vector[vector[string]]& token_vector)
