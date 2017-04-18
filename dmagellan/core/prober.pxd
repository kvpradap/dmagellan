from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp cimport bool
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap

from .tokencontainer cimport TokenContainer
from .invertedindex cimport InvertedIndex

cdef class Prober:
  cdef vector[pair[int, int]] pair_indices
  cdef int _size(self)
  cdef pair[int, int] _get_index(self, int i)
  cdef vector[int] _get_ltable_indices(self)
  cdef vector[int] _get_rtable_indices(self)
  cdef void _probe(self, TokenContainer inp_token_list, InvertedIndex index, int y) nogil
