import psycopg2
# connect to postgres database that was previously made (step 6 in readme.md)
conn = psycopg2.connect(database="final_project", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()
# this is raw data on bus stops with no header file (cleaned in bash.sh file)
file = open('/data/MIDS_w205_FinalProject/clean_data/bus/stops_noH.txt', 'r')
lines = file.readlines()
# loop through the files and pick out only information needed for our application (name of variables are relevant)
for line in lines:
    split_line = line.split(",")
    stop_id_py = int(split_line[0])
    stop_name_py = split_line[2]
    stop_lat_py = float(split_line[4])
    stop_lon_py = float(split_line[5])
    cur.execute("INSERT INTO bus_stop_seattle (stop_id, stop_name, stop_lat, stop_lon) VALUES (%s, %s, %s, %s);", (stop_id_py, stop_name_py, stop_lat_py, stop_lon_py))
    conn.commit()
    print(stop_id_py)
print("----------------------")
print("SCRIPT DONE")
