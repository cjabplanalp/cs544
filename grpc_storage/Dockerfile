FROM ubuntu:22.10

COPY server.py server.py
COPY numstore_pb2_grpc.py numstore_pb2_grpc.py
COPY numstore_pb2.py numstore_pb2.py

RUN apt-get update
RUN apt-get install -y python3 python3-pip curl lsof
RUN pip3 install grpcio
RUN pip3 install grpcio-tools

CMD ["python3", "server.py", "&"]