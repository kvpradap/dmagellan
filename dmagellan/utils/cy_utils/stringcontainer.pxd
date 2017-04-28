from libcpp.string cimport string
from libcpp.vector cimport vector

cdef class StringContainer:
    cdef vector[string] box
    cdef vector[int] ids
    cdef int csize(self)
