from libcpp.string cimport string
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap

cdef class StringContainer:
    cdef vector[pair[int,string]] container

    cdef void _push_back(self, int i, string s)
    cdef vector[pair[int,string]] _get(self)
    cdef pair[int, string] _get_index(self, int i) nogil
    cdef int _get_size(self) nogil
