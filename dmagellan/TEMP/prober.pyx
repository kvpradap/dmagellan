from cython.parallel cimport prange
from libcpp.algorithm cimport sort
from cython.operator cimport dereference, preincrement

cdef bool comp(const pair[int, int] l, const pair[int, int] r):
    return l.second > r.second

# probe
cdef class Prober:


    cdef int _size(self):
        return self.pair_indices.size()

    cdef pair[int, int] _get_index(self, int i):
        return self.pair_indices[i]

    cdef vector[int] _get_ltable_indices(self):
        cdef oset[int] set_indices
        cdef vector[int] l_indices
        cdef int n = self._size()
        cdef int i
        cdef oset[int].iterator it
        for i in xrange(n):
            set_indices.insert(self.pair_indices[i].first)
        it = set_indices.begin()
        while it != set_indices.end():
            l_indices.push_back(dereference(it))
            preincrement(it)
        sort(l_indices.begin(), l_indices.end())
        return l_indices

    cdef vector[int] _get_rtable_indices(self):
        cdef oset[int] set_indices
        cdef vector[int] r_indices
        cdef int n = self._size()
        cdef int i
        cdef oset[int].iterator it
        for i in xrange(n):
            set_indices.insert(self.pair_indices[i].second)
        it = set_indices.begin()
        while it != set_indices.end():
            r_indices.push_back(dereference(it))
            preincrement(it)
        sort(r_indices.begin(), r_indices.end())
        return r_indices




    # cdef void _probe(self, TokenContainer inp_token_list, InvertedIndex index, int y):





    # cdef vector[int] _probe_token(int uid, vector[int] candidates) nogil:
    #     cdef vector[int] vltable_indices
    #     cdef oset[int] sltable_indices
    #     cdef omap[int, int] candidate_overlap
    #     cdef int cand, k
    #     cdef vector[pair[int, int]] to_sort
    #     cdef pair[int, int] entry
    #
    #     for cand in candidates:
    #         candidate_overlap[cand] += 1
    #     for entry in candidate_overlap:
    #         to_sort.push_back(entry)
    #
    #     sort(to_sort.begin(), to_sort.end(), comp)
    #     k = 0
    #
    #     for entry in to_sort:
    #         sltable_indices.insert(entry.first)
    #         k += 1
    #         if y == k:
    #             break
    #     for k in sltable_indices:
    #         vltable_indices.push_back(k)
    #     return vltable_indices





    cdef void _probe(self, TokenContainer inp_token_list, InvertedIndex index, int y) nogil:
        cdef int m = inp_token_list._get_size()
        cdef int i, j, k, uid, cand
        cdef pair[int, vector[string]] p_id_tokens
        cdef vector[string] tokens
        cdef string token
        cdef vector[int] candidates
        cdef omap[int,int].iterator it, end
        cdef pair[int, int] entry
        cdef oset[int] sample_ltable_indices
        cdef omap[int, int] candidate_overlap
        cdef vector[pair[int, int]] to_sort

        for i in range(m):
            p_id_tokens = inp_token_list._get_index(i)
            uid = p_id_tokens.first
            tokens = p_id_tokens.second
            for j in range(tokens.size()):
                candidates = index._get_values(tokens[j])
                for cand in candidates:
                    candidate_overlap[cand] += 1
            for entry in candidate_overlap:
                to_sort.push_back(entry)
            #sort(to_sort.begin(), to_sort.end(), comp)
            k = 0
            for entry in to_sort:
                if y == k:
                    break
                sample_ltable_indices.insert(entry.first)
                k += 1

            for k in sample_ltable_indices:
                self.pair_indices.push_back(pair[int, int](to_sort[k].first, uid))
            # with gil:
            candidate_overlap.clear()
            sample_ltable_indices.clear()
            to_sort.clear()


            # n = tokens.size()
#             for j in range(n):
#                 token = tokens[j]
# #                 print(token)
#                 candidates = index._get_values(token)
#                 o = candidates.size()
#                 for k in range(o):
#                     candidate_overlap[candidates[k]] += 1
#             it = candidate_overlap.begin()
#             end = candidate_overlap.end()
#             while it != end:
#                 to_sort.push_back(dereference(it))
#                 preincrement(it)
#             sort(to_sort.begin(), to_sort.end(), comp)
#             q = 0
#
#             for k in range(to_sort.size()):
#                 self.pair_indices.push_back(pair[int, int](to_sort[k].first, uid))
#                 if q == y:
#                     break
#                 q = q + 1
#
#             candidate_overlap.clear()
#             to_sort.clear()

    def probe(self, TokenContainer inp_token_list, InvertedIndex index, int y):
        # with nogil:
        self._probe(inp_token_list, index, y)
    def get_ltable_indices(self):
        return self._get_ltable_indices()
    def get_rtable_indices(self):
        return self._get_rtable_indices()
    def get_all(self):
        return self.pair_indices
    def get_index(self, i):
        return self._get_index(i)
