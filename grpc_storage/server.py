from concurrent import futures
import numstore_pb2, numstore_pb2_grpc, grpc, math, threading

# PRESS ^C TO KILL SERVER, OR IF RUNNING...
#   1. lsof -i tcp
#   2. kill <PID>

key_vals = {}
total = 0

lru_cache = {}
cache_size = 10
evictions = []

lock_setNum = threading.Lock()
lock_hash = threading.Lock()

class NumStore(numstore_pb2_grpc.NumStoreServicer):

    def SetNum(self, request, context):
        global key_vals
        global total
        global lock_setNum

        with lock_setNum:
            if request.key in key_vals.keys():
                total -= key_vals[request.key]
            key_vals[request.key] = request.value
            total += request.value
            return numstore_pb2.SetNumResponse(total = total) # could be outside the lock

    def Fact(self, request, context):
        global lru_cache
        global cache_size
        global evictions
        global lock_hash

        if request.key not in key_vals.keys():
            # context.set_code(grpc.StatusCode.NOT_FOUND)
            # context.set_details('Key not found!')
            return numstore_pb2.FactResponse(hit = False, error = "Key not found!")
        
        # CACHE EVICTION POLICY: LRU
        if request.key in lru_cache.keys(): # key already exists in the cache, HIT
            with lock_hash:
                evictions.remove(request.key)
                evictions.append(request.key)
                return numstore_pb2.FactResponse(value = lru_cache[request.key], hit = True)

        else: # key needs to be added to the cache, MISS
            fact = math.factorial(key_vals[request.key])
            with lock_hash:
                lru_cache[request.key] = fact
                evictions.append(request.key)
                if len(lru_cache) > cache_size:
                    evicted = evictions.pop(0)
                    lru_cache.pop(evicted)
                return numstore_pb2.FactResponse(value = fact, hit = False)

def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
    numstore_pb2_grpc.add_NumStoreServicer_to_server(NumStore(), server)
    server.add_insecure_port('[::]:5440')
    # server.add_insecure_port('localhost:5440')
    server.start()
    print("started listening on port 5440")
    server.wait_for_termination()

if __name__ == "__main__":
    server()