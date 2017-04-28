
cdef class WhiteSpaceTokenizer(Tokenizer):
    def __init__(self, return_set=True, stopwords=[]):
        cdef string word
        self.return_set = return_set
        if len(stopwords):
            for word in stopwords:
                self.stopwords[word] = 0

    cdef vector[string] cremove_stopwords(self, vector[string]& itokens) nogil:
        cdef vector[string] otokens
        cdef string token
        for token in itokens:
            if (self.stopwords.find(token) == self.stopwords.end()):
                otokens.push_back(token)
        return otokens

    cdef vector[string] ctokenize_wd(self, const string& istring) nogil:
        cdef char* ptr1
        cdef char* pch = strtok_r(<char*> istring.c_str(), " \t\n", &ptr1)
        cdef oset[string] stokens
        cdef vector[string] otokens
        cdef string token
        if self.return_set:
          while pch != NULL:
              stokens.insert(string(pch))
              pch = strtok_r(NULL, " \t\n", &ptr1)
          for token in stokens:
              otokens.push_back(token)
        else:
          while pch != NULL:
              otokens.push_back(string(pch))
              pch = strtok_r(NULL, " \t\n", &ptr1)

        return otokens
    cdef vector[string] ctokenize(self, const string& istring) nogil:
        cdef vector[string] tokens = self.ctokenize_wd(istring)
        tokens = self.cremove_stopwords(tokens)
        return tokens

    def tokenize(self, const string& istring):
        return self.ctokenize(istring)
