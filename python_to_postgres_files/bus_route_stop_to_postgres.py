import psycopg2

conn = psycopg2.connect(database="final_project", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()



file = open('/data/MIDS_w205_FinalProject/clean_data/bus/bus_route_stops_noH.txt', 'r')
lines = file.readlines()

for line in lines:
    split_line = line.split(",")
    route_short_name_py = split_line[0]
    bus_stop_name_py = split_line[1]
    bus_stop_id_py = int(split_line[2])
    bus_stop_lat_py = float(split_line[3])
    bus_stop_lon_py = float(split_line[4])


    cur.execute("INSERT INTO bus_route_stop_seattle (route_short_name, stop_name, stop_id, stop_lat, stop_lon) VALUES (%s, %s, %s, %s, %s);", (route_short_name_py, bus_stop_name_py, bus_stop_id_py, bus_stop_lat_py, bus_stop_lon_py))
    conn.commit()

    print(route_short_name_py)
print("----------------------")
print("SCRIPT DONE")
