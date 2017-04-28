from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map as omap

cdef class DownSampleProber:
    cdef vector[int] llocs, rlocs

    cdef int clsize(self)
    cdef int crsize(self)
    cdef vector[int] cget_llocs(self)
    cdef vector[int] cget_rlocs(self)
    cdef void cprobe(self, vector[int]& ids, vector[vector[string]]& token_vector, omap[string, vector[int]]& index, int yparam) nogil


    cdef inline vector[int] cvalues(self, omap[string, vector[int]]& index, \
            string token) nogil:
        cdef vector[int] tmp
        if index.find(token) != index.end():
            return index[token]
        else:
            return tmp
