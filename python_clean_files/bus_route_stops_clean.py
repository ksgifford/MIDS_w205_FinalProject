from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
import csv

sc =SparkContext()
sqlContext = SQLContext(sc)

# create schema for all four dataframes associated with buses, currently one string, but will be split later in code
schema_Fstops = 'stop_id stop_name stop_lat stop_lon'
schema_Ftrips = 'route_id trip_id trip_headsign'
schema_Froutes = 'route_id agency_id route_short_name route_desc'
schema_stop_times = 'trip_id stop_id'
# list of all of the no header txt files associated with buses
file_list = ['/data/MIDS_w205_FinalProject/clean_data/bus/stops_noH.txt',
             '/data/MIDS_w205_FinalProject/clean_data/bus/trips_noH.txt',
             '/data/MIDS_w205_FinalProject/clean_data/bus/routes_noH.txt',
             '/data/MIDS_w205_FinalProject/clean_data/bus/stop_times_noH.txt']
hdfs_str_start = 'file://'
# loops through each file in above list and creates a temp name and registers the data as a dataframe
for file in file_list:
    temp_file_loc = hdfs_str_start + file
    temp_data_name = temp_file_loc.split("/")[-1].split("_noH")[0] + "_temp"
    print temp_file_loc
    lines_temp = sc.textFile(temp_file_loc)
    row_split = lines_temp.map(lambda l: l.split(','))
    if 'stops_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10]))
        column_map = row_split.map(lambda p: (p[0], p[2], p[4], p[5]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_Fstops.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
    if 'trips_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]))
        column_map = row_split.map(lambda p: (p[0], p[2], p[3]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_Ftrips.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
    if 'routes_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]))
        column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[4]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_Froutes.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
    if 'stop_times_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8]))
        column_map = row_split.map(lambda p: (p[0], p[3]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_stop_times.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
# convert data into aql based dataframe
Ftrips_df = sqlContext.sql('select * FROM  trips_temp')
Froutes_df = sqlContext.sql('select * FROM  routes_temp')
stop_times_df = sqlContext.sql('select * FROM  stop_times_temp')
Fstops_df = sqlContext.sql('select * FROM stops_temp')
# now goes into bus route and groups by the "route_short_name" then filters out the trip information
# this is then filtered down to unique identiers of stop id stops with distinct command in pyspark
# the distinct ids are then merged with stop ids and now we have for each bus route, we have ever 
# single stop that it makes throughout the day. 
#
# This was needed because sometimes the route changes throughout the day depending on the time,
# so we decided to associated a bus route number with any stop the bus utilizes throughout the day
#
# Once that is done it is converted to a textfile to be parsed into postgres later.
with open('/data/MIDS_w205_FinalProject/clean_data/bus/bus_stop_with_routes.txt', 'a') as outfile:
    for bus in Froutes_df.rdd.collect():
        print bus.route_short_name
        bus_route_to_trips = Froutes_df.filter(Froutes_df["route_short_name"] == bus.route_short_name).select('route_id').collect()[0][0]
        trip_id_to_stop_times_list = [i.trip_id for i in Ftrips_df.filter(Ftrips_df["route_id"] == bus_route_to_trips).select('trip_id').collect()]
        stop_list = [i.stop_id for i in stop_times_df.where(col("trip_id").isin(trip_id_to_stop_times_list)).select('stop_id').distinct().collect()]
        bus_stop_by_routes = Fstops_df.where(col("stop_id").isin(stop_list))
        for stop in bus_stop_by_routes.rdd.collect():
            outfile.write(bus.route_short_name + ',' + stop.stop_name + ',')
            outfile.write(stop.stop_id)
            outfile.write(',')
            outfile.write(stop.stop_lat)
            outfile.write(',')
            outfile.write(stop.stop_lon)
            outfile.write('\n')
