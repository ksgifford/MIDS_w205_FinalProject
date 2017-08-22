import psycopg2

conn = psycopg2.connect(database="final_project", user="postgres", password="pass", host="localhost", port="5432")
cur = conn.cursor()

file = open('/data/final_project/clean_data/weather/weather_noH.txt', 'r')
lines = file.readlines()

for line in lines:
    split_line = line.split(",")

    cal_date_py = split_line[1]
    temp_high_degF_py = int(split_line[2])
    temp_high_avg_py = int(split_line[3])
    temp_high_low_py = int(split_line[4])
    # dew_point_degF_high_py = int(split_line[5])
    # dew_point_degF_avg_py = int(split_line[6])
    # dew_point_degF_low_py = int(split_line[7])
    # humidity_percent_high_py = int(split_line[8])
    # humidity_percent_avg_py = int(split_line[9])
    # humidity_percent_low_py = int(split_line[10])
    # sea_level_press_in_high_py = float(split_line[11])
    # sea_level_press_in_avg_py = float(split_line[12])
    # sea_level_press_in_low_py = float(split_line[13])
    visiblity_mi_high_py = int(split_line[14])
    visiblity_mi_avg_py = int(split_line[15])
    visiblity_mi_low_py = int(split_line[16])
    wind_mph_high_py = int(split_line[17])
    wind_mph_avg_py = int(split_line[18])
    wind_mph_low_py = int(split_line[19])
    precipitation_in_sum_py = float(split_line[20])
    events_py = split_line[21]

    cur.execute("INSERT INTO weather_seattle (cal_date, temp_high_degF, temp_high_avg, temp_high_low, visiblity_mi_high, visiblity_mi_avg, visiblity_mi_low, wind_mph_high, wind_mph_avg, wind_mph_low, precipitation_in_sum, events) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);", (cal_date_py, temp_high_degF_py, temp_high_avg_py, temp_high_low_py, visiblity_mi_high_py, visiblity_mi_avg_py, visiblity_mi_low_py, wind_mph_high_py, wind_mph_avg_py, wind_mph_low_py, precipitation_in_sum_py, events_py))
    conn.commit()

    print(cal_date_py)
print("----------------------")
print("SCRIPT DONE")
