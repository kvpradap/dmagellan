import sys

from libcpp.algorithm cimport sort

from .invertedindex cimport InvertedIndex
from .tokencontainer cimport TokenContainer

cdef bool comp(const pair[int, int] l, const pair[int, int] r):
    return l.second > r.second

cdef bool ge_compare(double val1, double val2) nogil:
    return val1 >= val2

cdef class OverlapProber:
    cdef int csize(self):
        return self.pair_ids.size()

    cdef vector[pair[int, int]] cget_pairids(self):
        return self.pair_ids

    cdef pair[int, int] cget_pairid(self, int i):
        return self.pair_ids[i]




    cdef cprobe(self, vector[int]& ids, vector[vector[string]]& token_vector, omap[string, vector[int]]& index, double threshold) nogil:
        cdef int m, n
        cdef int i, j, k
        cdef vector[string] tokens
        cdef vector[int] candidates
        cdef omap[int, int] cand_overlap
        cdef pair[int, int] entry
        cdef int lid, rid
        cdef int cand

        n = token_vector.size()
        for i in xrange(n):
            tokens = token_vector[i]
            rid = ids[i]
            m = tokens.size()
            for j in xrange(m):
                candidates = self.cvalues(index, tokens[j])
                for cand in candidates:
                    cand_overlap[cand] += 1
            for entry in cand_overlap:
                if (ge_compare(<double>entry.second, threshold)):
                    self.pair_ids.push_back(pair[int, int](entry.first, i))
            cand_overlap.clear()
    def probe(self, TokenContainer objtc, InvertedIndex index, double threshold):
        with nogil:
            self.cprobe(objtc.ids, objtc.box, index.index, threshold)
    def get_pairids(self):
        return self.pair_ids
