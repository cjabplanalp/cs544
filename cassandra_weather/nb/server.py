from concurrent import futures
import station_pb2, station_pb2_grpc, grpc
from datetime import datetime
import cassandra
from cassandra import ConsistencyLevel
import numpy as np

from cassandra.cluster import Cluster
try:
    cluster = Cluster(['project-5-p5-cam-db-1', 'project-5-p5-cam-db-2', 'project-5-p5-cam-db-3'])
    cass = cluster.connect()
except Exception as e:
    print(e)

#### stations schema
        # CREATE TABLE weather.stations (
        # id text,
        # date date,
        # name text static,
        # record station_record,
        # PRIMARY KEY (id, date)
        # ) WITH CLUSTERING ORDER BY (date ASC)
class Station(station_pb2_grpc.StationServicer):
    def RecordTemps(self, request, context):
        insert_statement = cass.prepare("""
            INSERT INTO weather.stations 
            (id, date, record) 
            VALUES 
            (?, ?, {tmin:?, tmax:?})
            """)
        insert_statement.consistency_level = ConsistencyLevel.ONE
         # format string to date obj
        date_obj = datetime.strptime(request.date, "%Y%m%d")
        formatted_date = datetime.strftime(date_obj, "%Y-%m-%d")
        
        try:
            cass.execute(insert_statement, (request.station, formatted_date, request.tmin, request.tmax))
            return station_pb2.RecordTempsReply(error = None)
        except ValueError:
            return station_pb2.RecordTempsReply(error = "Encountered a ValueError exception in RecordTemps.")
        except cassandra.Unavailable:
            return station_pb2.RecordTempsReply(error = "Encountered a cassandra.Unavailabale exception in RecordTemps.")
        except:
            return station_pb2.RecordTempsReply(error = "An unknown error occurred in RecordTemps.")
        
    def StationMax(self, request, context):
        max_statement = cass.prepare("""
                SELECT MAX(record.tmax)
                FROM weather.stations
                WHERE id = ?
                """)
        max_statement.consistency_level = ConsistencyLevel.THREE
            
        try:
            max_temp = cass.execute(max_statement, [request.station])
            return station_pb2.StationMaxReply(tmax = max_temp.one()[0], error = None)
        except ValueError:
            return station_pb2.StationMaxReply(tmax = None, error = "Encountered a ValueError exception in StationMax.")
        except cassandra.Unavailable:
            return station_pb2.StationMaxReply(tmax = None, error = "Encountered a cassandra.Unavailabale exception in StationMax.")
        except:
            return station_pb2.StationMaxReply(tmax = None, error = "An unknown error occurred in StationMax.")
        
def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4), options=[("grpc.so_reuseport", 0)])
    station_pb2_grpc.add_StationServicer_to_server(Station(), server)
    server.add_insecure_port('[::]:5440')
    server.start()
    print("started listening on port 5440")
    server.wait_for_termination()

if __name__ == "__main__":
    server()