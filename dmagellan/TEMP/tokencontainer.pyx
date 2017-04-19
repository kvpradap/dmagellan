from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.pair cimport pair
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap
from cython.parallel cimport prange

from stringcontainer cimport StringContainer

cdef class TokenContainer:

    # cython functions
    cdef int _get_size(self) nogil:
        return self.container.size()

    cdef vector[pair[int,vector[string]]] _get(self):
        return self.container

    cdef pair[int, vector[string]] _get_index(self, int i) nogil:
        return self.container[i]

    cdef vector[string] _stokenize(self, const string& inp_string) nogil:
        cdef char* ptr1
        cdef char* pch = strtok_r(<char*> inp_string.c_str(), " \t\n", &ptr1)
        cdef oset[string] tokens
        cdef vector[string] out_tokens
        cdef string s
        while pch != NULL:
            tokens.insert(string(pch))
            pch = strtok_r(NULL, " \t\n", &ptr1)
        for s in tokens:
            out_tokens.push_back(s)
        return out_tokens

    cdef vector[string] _remove_stopwords(self, vector[string] &inp_tokens, const omap[string, int] &stop_words) nogil:
        cdef vector[string] out_tokens
        cdef string token
        for token in inp_tokens:
            if (stop_words.find(token) == stop_words.end()):
                out_tokens.push_back(token)
        return out_tokens

    cdef void _tokenize(self, StringContainer container, omap[string, int] stopwords) nogil:
        cdef int n = container._get_size()
        cdef int i
        cdef string s
        cdef int uid
        cdef pair[int, string] p
        cdef vector[string] out_tokens
        for i in xrange(n):
            self.container.push_back(pair[int, vector[string]]())
        for i in range(n):
            p = container._get_index(i)
            uid = p.first
            s = p.second
            out_tokens = self._stokenize(s)
            out_tokens = self._remove_stopwords(out_tokens, stopwords)
            self.container[i] = pair[int, vector[string]](uid, out_tokens)

    # python wrappers
    def get_index(self, int i):
        return self._get_index(i)

    def get_size(self):
        return self._get_size()

    def tokenize(self, StringContainer concat_strings, stopwords):
        import string as pstring
        cdef omap[string, int] stopword_map
        str2bytes = lambda x: x if isinstance(x, bytes) else x.encode('utf-8')
        if len(stopwords):
            for s in stopwords:
                stopword_map[s] = 0
        self._tokenize(concat_strings, stopword_map)
