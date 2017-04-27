cdef class QgramTokenizer(Tokenizer):
  def __init__(self, int qval=2, bool padding=True, char prefix_pad='#', char suffix_pad='$', bool return_set=False):
    self.qval = qval
    self.padding = padding
    self.prefix_pad = prefix_pad
    self.suffix_pad = suffix_pad
    self.return_set = return_set

  cdef vector[string] ctokenize(self, const string& istring) nogil:
    cdef string inp_str = istring
    cdef oset[string] tokens
    cdef vector[string] out_tokens

    if self.padding:
      inp_str = string(self.qval - 1, self.prefix_pad) + inp_str + string(self.qval - 1, self.suffix_pad)
    cdef unsigned int i, n = inp_str.length() - self.qval + 1
    if self.return_set:
      for i in xrange(n):
        tokens.insert(inp_str.substr(i, self.qval))
      for s in tokens:
        out_tokens.push_back(s)
    else:
      for i in xrange(n):
        out_tokens.push_back(inp_str.substr(i, self.qval))
    return out_tokens

  def tokenize(self, const string& istring):
      return self.ctokenize(istring)
