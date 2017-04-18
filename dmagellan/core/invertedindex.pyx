from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap



cdef class InvertedIndex:

    cdef vector[int] _get_values(self, string token) nogil:
        cdef vector[int] dummy
        if self.index.find(token) != self.index.end():
            return self.index[token]
        else:
            return dummy
    cdef int _size(self) nogil:
        return self.index.size()


    cdef void _build_inv_index(self, TokenContainer objtc) nogil:
        cdef int n = objtc._get_size()
        cdef int m
        cdef int i,j
        cdef int idx
        cdef vector[string] tokens
        cdef string token
        cdef pair[int, vector[string]] p
        for i in xrange(n):
            p = objtc._get_index(i)
            idx = p.first
            tokens = p.second
            m = tokens.size()
            for j in xrange(m):
                self.index[tokens[j]].push_back(idx)


    def build_inv_index(self, objtc):
        self._build_inv_index(objtc)

    def size(self):
        return self._size()

    def get_values(self, token):
        return self._get_values(token)
    def get_inv_index(self):
        return self.index
