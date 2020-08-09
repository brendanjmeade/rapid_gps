import os
import shutil
import pandas as pd
import re

src = "E:/5_minute_gps/5_min_gps(total)/5_min_gps/2013"
dst = "E:/5_minute_gps/Target2"


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

# This is making a list of the desired sites
sites = query_locations(min_lat, max_lat, min_lon, max_lon)['site']

# print(*sites)

files = []
search_items = '|'.join(sites+'_fix.kenv')

rx = re.compile(rf'\.{search_items}')

print('Starting the collection of files into a list')
for path, dnames, fnames in os.walk(src):
    files.extend([os.path.join(path, x) for x in fnames if rx.search(x)])
    print(len(files))


print("Starting to copy over files to destination")
for ori in files:
    print(ori)
    shutil.copy(ori, dst)
