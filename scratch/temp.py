from distributed import Client

def inc(x):
     return x + 1

if __name__=='__main__':
     #client = Client('127.0.0.1:8786')
     client = Client('128.110.153.177:8786')
     with client:
        x = client.submit(inc, 10)
        print(x.result())
