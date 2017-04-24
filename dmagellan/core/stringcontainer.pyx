import sys
cdef class StringContainer:
    cdef int csize(self):
        return self.ids.size()
    
    def size(self):
        return self.csize()
    def push_back(self, int i, string s):
        self.ids.push_back(i)
        self.box.push_back(s)

    def get(self, int i):
        return (self.ids[i], self.box[i])

    def __getstate__(self):
        return (self.ids, self.box)
    def __setstate__(self, state):
        ids, box= state
        self.ids = ids
        self.box = box
    def __sizeof__(self):
        tmpbox = self.box
        tmpids = self.ids
        return sys.getsizeof(tmpbox) + sys.getsizeof(tmpids)
