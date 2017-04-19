from libcpp.string cimport basestring
from libcpp.vector cimport vector

cdef class StringContainer:
    cdef vector[string] sc
