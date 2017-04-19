from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap

from .tokencontainer cimport TokenContainer


cdef class InvertedIndex:
    cdef omap[string, vector[int]] index
    cdef vector[int] _get_values(self, string token) nogil
    cdef int _size(self) nogil
    cdef void _build_inv_index(self, TokenContainer objtc) nogil
