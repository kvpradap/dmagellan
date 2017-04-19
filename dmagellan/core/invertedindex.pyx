import sys
from .tokencontainer cimport TokenContainer
cdef class InvertedIndex:
    cdef vector[int] cvalues(self, string token):
        cdef vector[int] tmp
        if self.index.find(token) != self.index.end():
            return self.index[token]
        else:
            return tmp
    
    cdef void cbuild_inv_index(self, vector[vector[string]]& token_vector):
        cdef int n = token_vector.size()
        cdef int i, j, m
        cdef vector[string] tokens

        for i in xrange(n):
            tokens = token_vector[i]
            m = tokens.size()
            for j in xrange(m):
                self.index[tokens[j]].push_back(i)
    
    def build_inv_index(self, TokenContainer objtc):
        self.cbuild_inv_index(objtc.box)

    def values(self, token):
        return self.cvalues(token)

    def __getstate__(self):
        return self.index

    def __setstate__(self, state):
        self.index = state

    def __sizeof__(self):
        # this is an overestimation.
        x = self.index
        return sys.getsizeof(x)

