#!/bin/bash
# This script fully installs the Transit Risk Pipeline.

wget http://metro.kingcounty.gov/GTFS/google_daily_transit.zip -P /data/MIDS_w205_FinalProject/raw_data/bus

wait

unzip -d /data/MIDS_w205_FinalProject/raw_data/bus /data/MIDS_w205_FinalProject/raw_data/bus/google_daily_transit.zip

wait

# Downloads the 911 call data file which we will use as proxy for crime activity in Seattle
wget -O /data/MIDS_w205_FinalProject/raw_data/crime/Seattle_Police_Department_911_Incident_Response.csv https://data.seattle.gov/api/views/3k2p-39jp/rows.csv?accessType=DOWNLOAD

# Ensures the script will wait for download of crime data before proceeding
wait

# This section clears out the header records from the data files
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/routes.txt > /data/MIDS_w205_FinalProject/clean_data/bus/routes_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/stops.txt > /data/MIDS_w205_FinalProject/clean_data/bus/stops_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/trips.txt > /data/MIDS_w205_FinalProject/clean_data/bus/trips_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/bus/stop_times.txt > /data/MIDS_w205_FinalProject/clean_data/bus/stop_times_noH.txt
tail -n +2 /data/MIDS_w205_FinalProject/raw_data/crime/Seattle_Police_Department_911_Incident_Response.csv > /data/MIDS_w205_FinalProject/clean_data/crime/Seattle_Police_Department_911_Incident_Response_noH.csv


wait

# Makes the bus_route_stop_clean script executable and then runs it
chmod +x /data/MIDS_w205_FinalProject/python_clean_files/bus_route_stops_clean.py
/home/w205/spark15/bin/spark-submit /data/MIDS_w205_FinalProject/python_clean_files/bus_route_stops_clean.py

# Ensures script blocks to wait for bus_route_stops_clean step
wait

# Downloads and installs required python libraries
wget https://repo.continuum.io/archive/Anaconda2-4.1.1-Linux-x86_64.sh
chmod +x *.sh
./Anaconda2-4.1.1-Linux-x86_64.sh
/root/anaconda2/bin/pip install psycopg2==2.6.2
/root/anaconda2/bin/pip install geopandas
/root/anaconda2/bin/pip install sqlalchemy
sudo yum install geos-devel


# This section runs the sql scripts which create the tables and necessary data for the pipeline in postgres
chmod +x /data/MIDS_w205_FinalProject/sql_files/sql_file_creation.sql
psql --username=postgres -d final_project -f /data/MIDS_w205_FinalProject/sql_files/sql_file_creation.sql

# Runs python routines that operate on postgres data uploaded in previous step
chmod +x /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_route_stop_to_postgres.py
chmod +x /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_stop_to_postgres.py
python /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_route_stop_to_postgres.py
python /data/MIDS_w205_FinalProject/python_to_postgres_files/bus_stop_to_postgres.py

#### GEOSPATIAL PROCESSING
pip install pandas
source ~/.bashrc
conda install -c ioos rtree
chmod +x /data/MIDS_w205_FinalProject/spatial/geodf_processing.py
/root/anaconda2/bin/python /data/MIDS_w205_FinalProject/spatial/geodf_processing.py


echo '------------------------------------'
echo '------------------------------------'
echo '------------SCRIPT DONE-------------'
echo '------------------------------------'
echo '------------------------------------'
