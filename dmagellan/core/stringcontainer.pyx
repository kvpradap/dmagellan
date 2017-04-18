from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap
from cython.parallel cimport prange

cdef class StringContainer:
    cdef void _push_back(self, int i, string s):
        self.container.push_back((i, s))
    cdef vector[pair[int,string]] _get(self):
        return self.container
    cdef pair[int, string] _get_index(self, int i) nogil:
        return self.container[i]
    cdef int _get_size(self) nogil:
        return self.container.size()

    # python wrappers
    def push_back(self, int i, string s):
        self.container.push_back((i,s))
    def get(self):
        return self.container
    def get_index(self, int i):
        return self.container[i]
    def get_size(self):
        return self._get_size()
