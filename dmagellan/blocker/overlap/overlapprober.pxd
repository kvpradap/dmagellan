from libcpp.vector cimport vector
from libcpp.string cimport string
from libcpp.map cimport map as omap
from libcpp.set cimport set as oset
from libcpp.pair cimport pair
from libcpp cimport bool


cdef class OverlapProber:
    cdef vector[pair[int, int]] pair_ids

    cdef int csize(self)
    cdef vector[pair[int, int]] cget_pairids(self)
    cdef pair[int, int] cget_pairid(self, int i)

    cdef void cprobe(self, vector[int]& ids, vector[vector[string]]& token_vector, omap[string, vector[int]]& index, double threshold) nogil

    cdef inline vector[int] cvalues(self, omap[string, vector[int]]& index, string token) nogil:
        cdef vector[int] tmp
        if index.find(token) != index.end():
            return index[token]
        else:
            return tmp
