#!/bin/bash

wget http://metro.kingcounty.gov/GTFS/google_daily_transit.zip -P /data/MIDS_w205_FinalProject/raw_data/bus
unzip -d /data/MIDS_w205_FinalProject/raw_data/bus /data/MIDS_w205_FinalProject/raw_data/bus/google_daily_transit.zip
wget -O /data/MIDS_w205_FinalProject/raw_data/crime/Seattle_Police_Department_911_Incident_Response.csv https://data.seattle.gov/d/3k2p-39jp?category=Public-Safety&view_name=Seattle-Police-Department-911-Incident-Response

tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/routes.txt > /data/MIDS_w205_FinalProject/clean_data/bus/routes_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/stops.txt > /data/MIDS_w205_FinalProject/clean_data/bus/stops_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/trips.txt > /data/MIDS_w205_FinalProject/clean_data/bus/trips_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/stop_times.txt > /data/MIDS_w205_FinalProject/clean_data/bus/stop_times_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/crime/Seattle_Police_Department_911_Incident_Response.csv > /data/MIDS_w205_FinalProject/clean_data/crime/Seattle_Police_Department_911_Incident_Response_noH.csv

chmod +x /data/MIDS_w205_FinalProject/python_clean_files/bus_route_stops_clean.py
/home/w205/spark15/bin/spark-submit /data/MIDS_w205_FinalProject/python_clean_files/bus_route_stops_clean.py

wget https://repo.continuum.io/archive/Anaconda2-4.1.1-Linux-x86_64.sh
chmod +x *.sh
./Anaconda2-4.1.1-Linux-x86_64.sh
/root/anaconda2/bin/pip install psycopg2==2.6.2
/root/anaconda2/bin/pip install geopandas
/root/anaconda2/bin/pip install sqlalchemy
sudo yum install geos-devel


chmod +x /data/MIDS_w205_FinalProject/sql_files/sql_file_creation.sql
psql --username=postgres -d MIDS_w205_FinalProject -f /data/MIDS_w205_FinalProject/sql_files/sql_file_creation.sql

chmod +x /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_route_stop_to_postgres.py
chmod +x /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_stop_to_postgres.py
python /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_route_stop_to_postgres.py
python /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_stop_to_postgres.py

#### GEOSPATIAL PROCESSING
python /data/MIDS_w205_FinalProject/spatial/geodf_processing.py
