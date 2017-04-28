cdef class Tokenizer:
    cdef vector[string] ctokenize(self, string s) nogil:
        pass