import sys
from .stringcontainer cimport StringContainer
from .tokenizer cimport Tokenizer
cdef class TokenContainer:

    cdef int csize(self):
        return self.ids.size()

    cdef void cinit(self, int n) nogil:
        cdef int i
        for i in xrange(n):
            self.ids.push_back(int())
            self.box.push_back(vector[string]())

    cdef void cpush_back(self, int i, vector[string] tokens):
        self.ids.push_back(i)
        self.box.push_back(tokens)

    # cdef vector[string] cremove_stopwords(self, vector[string]& itokens, \
    #         omap[string, int]& stopwords) nogil:
    #     cdef vector[string] otokens
    #     cdef string token
    #     for token in itokens:
    #         if (stopwords.find(token) == stopwords.end()):
    #             otokens.push_back(token)
    #     return otokens
    #
    # cdef vector[string] ctokenize_wd(self, const string& istring) nogil:
    #     cdef char* ptr1
    #     cdef char* pch = strtok_r(<char*> istring.c_str(), " \t\n", &ptr1)
    #     cdef oset[string] stokens
    #     cdef vector[string] otokens
    #     cdef string token
    #     while pch != NULL:
    #         stokens.insert(string(pch))
    #         pch = strtok_r(NULL, " \t\n", &ptr1)
    #     for token in stokens:
    #         otokens.push_back(token)
    #     return otokens

    cdef void ctokenize(self, vector[int]& ids, vector[string]& istrings, \
           Tokenizer objtok) nogil:
        cdef int n = istrings.size()
        cdef int i
        cdef string istring
        cdef vector[string] tokens

        self.cinit(n)
        for i in xrange(n):
            istring = istrings[i]
            # tokens = self.ctokenize_wd(istring)
            # tokens = self.cremove_stopwords(tokens, stopwords)
            tokens = objtok.ctokenize(istring)
            self.box[i] = tokens
            self.ids[i] = ids[i]



    def tokenize(self, StringContainer objsc, Tokenizer objtok):
        with nogil:
            self.ctokenize(objsc.ids, objsc.box, objtok)

    def get(self, int i):
        return (self.ids[i], self.box[i])

    def size(self):
        return self.csize()

    def __getstate__(self):
        return (self.ids, self.box)

    def __setstate__(self, state):
        ids, box = state
        self.ids = ids
        self.box = box


    def __sizeof__(self):
        x = self.box
        y = self.ids
        return sys.getsizeof(x) + sys.getsizeof(y)
