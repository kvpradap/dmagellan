import sys
cdef class StringContainer:
    cdef int csize(self):
        return self.box.size()
    
    def size(self):
        return self.csize()
    def push_back(self, s):
        self.box.push_back(s)
    def get(self, int i):
        return self.box[i]

    def __getstate__(self):
        return self.box
    def __setstate__(self, state):
        self.box = state
    def __sizeof__(self):
        tmp = self.box
        return sys.getsizeof(tmp)
