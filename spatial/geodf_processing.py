# Code adapted from the following:
# https://gis.stackexchange.com/a/165413
# https://gis.stackexchange.com/questions/174159/convert-a-pandas-dataframe-to-a-geodataframe/174168
# https://stackoverflow.com/a/38363154


#Importing necessary dependencies
from pandas import *
from sqlalchemy import create_engine
from geopandas import GeoDataFrame
from geopandas.tools import sjoin
from shapely.geometry import Point, Polygon, shape

#Function to generate WKB hex value
def wkb_hexer(line):
    return line.wkb_hex

#Establish connection to the postres database
engine = create_engine('postgresql://postgres@localhost:5432/final_project')

#Build pandas dataframes from postgres tables
print 'Building dataframes...'
df_bus = read_sql_query('select * from "bus_stop_seattle"',con=engine)
df_crime = read_sql_query('select cad_cdw_id, latitude, longitude from "seattle_crime"', con=engine)

#Build Geodataframes from pandas dataframes
geo_bus = [Point(xy) for xy in zip(df_bus.stop_lon, df_bus.stop_lat)]
df_bus = df_bus.drop(['stop_lon', 'stop_lat'], axis=1)

geo_crime = [Point(xy) for xy in zip(df_crime.longitude, df_crime.latitude)]
df_crime = df_crime.drop(['longitude', 'latitude'], axis=1)

crs = {'init': 'epsg:4326'}
geodf_bus = GeoDataFrame(df_bus, crs=crs, geometry=geo_bus)
geodf_crime = GeoDataFrame(df_crime, crs=crs, geometry=geo_crime)

#Reproject lat/long coordinates into Washington State Plane
print 'Reprojecting coordinate system...'
state_plane = {'init': 'epsg:2285'}

geodf_bus = geodf_bus.to_crs(state_plane)
geodf_crime = geodf_crime.to_crs(state_plane)

#Create buffer area around transit stops
print 'Buffering transit stops...'
geodf_bus['geometry'] = geodf_bus.buffer(250)

#Perform spatial join
print 'Performing spatial join...'
geodf_spjoin = sjoin(geodf_crime, geodf_bus, how = 'inner')
print(geodf_spjoin.head())

#Store output as ESRI shapefile
make_shp = raw_input('Would you like to create shapefiles? This could take a while. (y/n): ')
if make_shp.upper() == 'Y':
    print 'Writing shapefiles...'
    geodf_bus.to_file(driver = 'ESRI Shapefile', filename = './bus_stop_buffer.shp')
    geodf_crime.to_file(driver = 'ESRI Shapefile', filename = './crime_points.shp')
    geodf_spjoin.to_file(driver = 'ESRI Shapefile', filename = './spatial_join.shp')
    print 'Shapefile export complete.'
else: print 'Skipping shapefile export.'

#Output dataframe to postgres
print 'Writing spatial join geodataframe to postgres...'
geodf_spjoin['geometry'] = geodf_spjoin['geometry'].apply(wkb_hexer)
geodf_spjoin.to_sql('spjoin', engine, if_exists = 'replace', index=False)

print 'COMPLETE!'
