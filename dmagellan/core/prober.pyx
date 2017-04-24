import sys

from libcpp.algorithm cimport sort
from libcpp cimport bool
from libcpp.set cimport set as oset
from libcpp.pair cimport pair
from libcpp.vector cimport vector
from libcpp.map cimport map as omap

from .invertedindex cimport InvertedIndex
from .tokencontainer cimport TokenContainer

cdef bool comp(const pair[int, int] l, const pair[int, int] r):
        return l.second > r.second

cdef class Prober:
    cdef int clsize(self):
        return self.llocs.size()
    cdef int crsize(self):
        return self.rlocs.size()

    cdef vector[int] cget_llocs(self):
        sort(self.llocs.begin(), self.llocs.end())
        return self.llocs

    cdef vector[int] cget_rlocs(self):
        sort(self.rlocs.begin(), self.rlocs.end())
        return self.rlocs

    cdef void cprobe(self, vector[int]& ids, vector[vector[string]]& token_vector, \
            omap[string, vector[int]]& index,\
            int yparam):
        cdef int m, n
        cdef int i, j, k
        cdef vector[string] tokens
        cdef oset[int] lset, rset
        cdef vector[int] candidates
        cdef omap[int, int] cand_overlap
        cdef pair[int, int] entry
        cdef vector[pair[int, int]] tmp
        cdef int rid
        cdef int mx = 0
        n = token_vector.size()

        for i in xrange(n):
            tokens = token_vector[i]
            rid = ids[i]
            m = tokens.size()
            for j in xrange(m):
                candidates = self.cvalues(index, tokens[j])
                #if candidates.size() >  mx:
                #    mx = candidates.size()
                #    print(tokens[j])
                #    print(mx)
                for cand in candidates:
                    cand_overlap[cand] += 1
            if cand_overlap.size():
                rset.insert(rid)
            for entry in cand_overlap:
                tmp.push_back(entry)
            sort(tmp.begin(), tmp.end(), comp)
            k = 0
            for entry in tmp:
                lset.insert(entry.first)
                k += 1
                if k == yparam:
                    break
            cand_overlap.clear()
            tmp.clear()
        for i in lset:
            self.llocs.push_back(i)
        for i in rset:
            self.rlocs.push_back(i)

    def probe(self, TokenContainer objtc, InvertedIndex index, int yparam):
        #with nogil:
        self.cprobe(objtc.ids, objtc.box, index.index, yparam)

    def get_lids(self):
        return self.cget_llocs()
    def get_rids(self):
        return self.cget_rlocs()

    def __getstate__(self):
        return (self.llocs, self.rlocs)
    def __setstate__(self, state):
        llocs, rlocs = state
        self.llocs = llocs
        self.rlocs = rlocs

    def __sizeof__(self):
        x = self.llocs
        y = self.rlocs
        return sys.getsizeof(x) + sys.getsizeof(y)

        

