from libcpp.string cimport string
from libcpp.vector cimport vector
from libcpp.set cimport set as oset
from libcpp.map cimport map as omap
from libcpp cimport bool
from .tokenizer cimport Tokenizer

cdef class QgramTokenizer(Tokenizer):
  cdef int qval
  cdef char prefix_pad, suffix_pad
  cdef bool padding, return_set

  cdef vector[string] ctokenize(self, const string& istring) nogil
