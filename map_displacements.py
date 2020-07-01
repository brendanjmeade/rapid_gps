import datetime
import pandas as pd
import numpy as np
from dask import dataframe as dd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata

# read in the station info
stations = pd.read_csv('locations.txt', sep='  ', engine='python', names=['site', 'lat', 'lon', 'height'])

lon = stations['lon']
lon = lon.where(lon > -180, other=lon+360) #a lot of the longitudes are less than -180. this makes them between -180 and +180
stations.drop(columns=['lon'])
stations['lon'] = lon


def query_locations(min_lat, max_lat, min_lon, max_lon):
    # returns only the stations in a given area, based on limits of latitude and longitude
    region = stations.query(str(min_lat) + ' < lat < ' + str(max_lat)).query(str(min_lon) + ' < lon < ' + str(max_lon))
    return region


# assign the desired limits for latitude and longitude
min_lat = 32
max_lat = 36
min_lon = -120
max_lon = -114

socal = query_locations(min_lat, max_lat, min_lon, max_lon)

# read in the gps data
df = dd.read_parquet('/Users/amyferrick/Downloads/5_min_GPS_subset/unr_5min_gps.parquet')
# only choose the sites that are in the desired region
df = df[df['site'].isin(socal['site'])]
# add the lat and lon columns to the gps dataframe
df = df.join(socal.set_index('site'), on='site')



# get data at a certain time
# start = 'datetime.datetime(2015, 1, 11, 2, 5, 0)'
# df = df.query('date_time == ' + start)
df = df.compute()


# plot interpolation of data at that time
grid_x, grid_y = np.mgrid[min_lat:max_lat:100j, min_lon:max_lon:200j]

points_X = df[['lon']]
points_Y = df[['lat']]
points = df[['lat', 'lon']].values


grid_e = griddata(points, df['e_ref'], (grid_x, grid_y), method='linear')
grid_n = griddata(points, df['n_ref'], (grid_x, grid_y), method='linear')
grid_v = griddata(points, df['v_ref'], (grid_x, grid_y), method='linear')

plt.subplot(131)
plt.imshow(grid_e.T, extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
plt.plot(points_X, points_Y, "k.", ms=.25)
plt.title('East Displacement')
plt.subplot(132)
plt.imshow(grid_n.T, extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
plt.plot(points_X, points_Y, "k.", ms=.25)
plt.title('North Displacement')
plt.subplot(133)
plt.imshow(grid_v.T, extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
plt.plot(points_X, points_Y, "k.", ms=.25)
plt.title('Up Displacement')
plt.show()
