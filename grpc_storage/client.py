import sys, grpc, threading, random, time
import numstore_pb2, numstore_pb2_grpc
import numpy as np

hits = []
lock = threading.Lock()
times = []

port = sys.argv[1]
addr = f"127.0.0.1:{port}"

def generate_request_key():
    all_keys = [str(i) for i in range(1, 101)]
    return random.choice(all_keys)

def send_request():
    global hits
    global lock
    global times

    with grpc.insecure_channel(addr) as channel:
        stub = numstore_pb2_grpc.NumStoreStub(channel)

        for i in range(100):
            # SetNum
            if random.random() > 0.5:
                setNum_values = list(range(1, 15))
                with lock:
                    chosen_value = random.choice(setNum_values)
                    start = time.time()
                    response = stub.SetNum(numstore_pb2.SetNumRequest(key = generate_request_key(), value = chosen_value))
                    end = time.time()
                # with lock:
                    times.append((end-start) * 1000)
            # Fact
            else:
                with lock:
                    start = time.time()
                    response = stub.Fact(numstore_pb2.FactRequest(key = generate_request_key()))
                    end = time.time()
                # with lock:
                    times.append((end - start) * 1000)
                    if response.error != "Key not found!":
                        hits.append(response.hit)

if __name__ == "__main__":
    t1 = threading.Thread(target = send_request)
    t2 = threading.Thread(target = send_request)
    t3 = threading.Thread(target = send_request)
    t4 = threading.Thread(target = send_request)
    t5 = threading.Thread(target = send_request)
    t6 = threading.Thread(target = send_request)
    t7 = threading.Thread(target = send_request)
    t8 = threading.Thread(target = send_request)


    t1.start()
    t2.start()
    t3.start()
    t4.start()
    t5.start()
    t6.start()
    t7.start()
    t8.start()

    t1.join()
    t2.join()
    t3.join()
    t4.join()
    t5.join()
    t6.join()
    t7.join()
    t8.join()

    print("p50 response time: " + str(np.percentile(times, 50)), "milliseconds")
    print("p99 response time: " + str(np.percentile(times, 99)), "milliseconds")
    print("Cache hit rate: " + str(sum(hits) / len(hits)))
