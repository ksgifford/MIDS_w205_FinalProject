from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from pyspark.sql.functions import col
import csv

sc =SparkContext()
sqlContext = SQLContext(sc)


# schema_Fstops = 'stop_id stop_code stop_name stop_desc stop_lat stop_lon zone_id stop_url location_type parent_station stop_timezone'
schema_Fstops = 'stop_id stop_name stop_lat stop_lon'
# schema_Ftrips = 'route_id service_id trip_id trip_headsign trip_short_name direction_id block_id shape_id peak_flag fare_id'
schema_Ftrips = 'route_id trip_id trip_headsign'
# schema_Froutes = 'route_id agency_id route_short_name route_long_name route_desc route_type route_url route_color route_text_color'
schema_Froutes = 'route_id agency_id route_short_name route_desc'
# schema_stop_times = 'trip_id arrival_time departure_time stop_id stop_sequence stop_headsign pickup_type drop_off_type shape_dist_traveled'
schema_stop_times = 'trip_id stop_id'
file_list = ['/data/MIDS_w205_FinalProject/raw_data/bus/Fstops_noH.txt',
             '/data/MIDS_w205_FinalProject/raw_data/bus/Ftrips_noH.txt',
             '/data/MIDS_w205_FinalProject/raw_data/bus/Froutes_noH.txt',
             '/data/MIDS_w205_FinalProject/raw_data/bus/stop_times_noH.txt']
hdfs_str_start = 'file://'


for file in file_list:
    temp_file_loc = hdfs_str_start + file
    temp_data_name = temp_file_loc.split("/")[-1].split("_noH")[0] + "_temp"
    print temp_file_loc
    lines_temp = sc.textFile(temp_file_loc)
    row_split = lines_temp.map(lambda l: l.split(','))
    if 'Fstops_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9], p[10]))
        column_map = row_split.map(lambda p: (p[0], p[2], p[4], p[5]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_Fstops.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
    if 'Ftrips_noH' in file:
        # column_map = row_split.map(lambda p: (p[0], p[1], p[2], p[3], p[4], p[5], p[6], p[7], p[8], p[9]))
        column_map = row_split.map(lambda p: (p[0], p[2], p[3]))
        fields = [StructField(field_name, StringType(), True) for field_name in schema_Ftrips.split()]
        schema = StructType(fields)
        schema_data_temp = sqlContext.createDataFrame(column_map, schema)
        schema_data_temp.registerTempTable(temp_data_name)
    if 'Froutes_noH' in file:
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

Ftrips_df = sqlContext.sql('select * FROM  Ftrips_temp')
Froutes_df = sqlContext.sql('select * FROM  Froutes_temp')
stop_times_df = sqlContext.sql('select * FROM  stop_times_temp')
Fstops_df = sqlContext.sql('select * FROM  Fstops_temp')

bus_stop = sqlContext.sql('select * FROM Fstops_temp WHERE stop_id = 1110')
# bus_stop.show()

bus_stop_info = {}
for stop in bus_stop.rdd.collect():
    bus_stop_info[stop.stop_id] = {}
    bus_stop_info[stop.stop_id]['stop_name'] = stop.stop_name
    bus_stop_info[stop.stop_id]['stop_lat'] = stop.stop_lat
    bus_stop_info[stop.stop_id]['stop_lon'] = stop.stop_lon

print bus_stop_info


# for bus in Froutes_df.rdd.collect():
with open('/data/MIDS_w205_FinalProject/clean_data/bus/bus_route_stops_noH.txt', 'a') as outfile:
    for bus in Froutes_df.rdd.collect():
        print bus.route_short_name
        bus_route_to_trips = Froutes_df.filter(Froutes_df["route_short_name"] == bus.route_short_name).select('route_id').collect()[0][0]
        # bus_route_to_trips
        trip_id_to_stop_times_list = [i.trip_id for i in Ftrips_df.filter(Ftrips_df["route_id"] == bus_route_to_trips).select('trip_id').collect()]
        # trip_id_to_stop_times_list
        stop_list = [i.stop_id for i in stop_times_df.where(col("trip_id").isin(trip_id_to_stop_times_list)).select('stop_id').distinct().collect()]
        # stop_list
        bus_stop_by_routes = Fstops_df.where(col("stop_id").isin(stop_list))
        # bus_stop_by_routes.show(100)
        # outfile.write(str(bus.route_short_name) + ',')
        # bus_stop_by_routes_info = {}
        for stop in bus_stop_by_routes.rdd.collect():
            # bus_stop_by_routes_info[stop.stop_id] = {}
            # bus_stop_by_routes_info[stop.stop_id]['stop_name'] = stop.stop_name
            # bus_stop_by_routes_info[stop.stop_id]['stop_lat'] = stop.stop_lat
            # bus_stop_by_routes_info[stop.stop_id]['stop_lon'] = stop.stop_lon
            outfile.write(bus.route_short_name + ',' + stop.stop_name + ',')
            outfile.write(stop.stop_id)
            outfile.write(',')
            outfile.write(stop.stop_lat)
            outfile.write(',')
            outfile.write(stop.stop_lon)
            outfile.write('\n')
        # outfile.write(str(bus_stop_by_routes_info))
        # outfile.write('\n')


    # bus_stop_by_routes_info.saveAsTextFile('file///home/w205/test.txt')
