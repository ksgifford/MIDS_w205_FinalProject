import psycopg2

conn = psycopg2.connect(database="final_project", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()



file = open('/data/final_project/clean_data/bus/Fstops_noH.txt', 'r')
lines = file.readlines()

for line in lines:
    split_line = line.split(",")

    stop_id_py = int(split_line[0])
    # stop_code_py = split_line[1]
    stop_name_py = split_line[2]
    # stop_desc_py = split_line[3]
    stop_lat_py = float(split_line[4])
    stop_lon_py = float(split_line[5])
    # zone_id_py = split_line[6]
    # stop_url_py = split_line[7]
    # location_type_py = float(split_line[8])
    # parent_station_py = split_line[9]
    # stop_timezone_py = split_line[10]

    cur.execute("INSERT INTO bus_stop_seattle (stop_id, stop_name, stop_lat, stop_lon) VALUES (%s, %s, %s, %s);", (stop_id_py, stop_name_py, stop_lat_py, stop_lon_py))
    conn.commit()

    print(stop_id_py)
print("----------------------")
print("SCRIPT DONE")
