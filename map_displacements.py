import datetime
import pandas as pd
import numpy as np
from dask import dataframe as dd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata

# assign desired time interval
start = 'datetime.datetime(2016, 6, 10, 0, 0, 0)'
start0 = datetime.datetime(2016, 6, 10, 0, 0, 0)
end = 'datetime.datetime(2016, 6, 11, 0, 0, 0)'
# assign the desired limits for latitude and longitude
min_lat = 37
max_lat = 39
min_lon = -123.5
max_lon = -121.5

# read in the station info
stations = pd.read_csv('locations.txt', sep='  ', engine='python', names=['site', 'lat', 'lon', 'height'])

# convert longitude array to conventional values
lon = stations['lon']
lon = lon.where(lon > -180, other=lon+360) #a lot of the longitudes are less than -180. this makes them between -180 and +180
stations.drop(columns=['lon'])
stations['lon'] = lon

def query_locations(min_lat, max_lat, min_lon, max_lon):
    # returns only the stations in a given area, based on limits of latitude and longitude
    region = stations.query(str(min_lat) + ' < lat < ' + str(max_lat)).query(str(min_lon) + ' < lon < ' + str(max_lon))
    return region

def normalize(df):
    # normalizes displacements at each station with respect to the mean at each station
    sites = df.site.unique()
    for i in sites:
        df_site = df.loc[df['site'] == i]
        e_mean = np.mean(df_site.e_ref)
        n_mean = np.mean(df_site.n_ref)
        v_mean = np.mean(df_site.v_ref)
        df.loc[df['site'] == i, 'e_ref'] -= e_mean
        df.loc[df['site'] == i, 'n_ref'] -= n_mean
        df.loc[df['site'] == i, 'v_ref'] -= v_mean
    #method1
    #df.e_ref = df.e_ref - (df.e_ref.mean())
    #df.v_ref = df.v_ref - (df.v_ref.mean())
    #df.n_ref = df.n_ref - (df.n_ref.mean())
    return df

# read in the gps data
df = dd.read_parquet('/Users/amyferrick/Downloads/2014_subset.parquet')

socal = query_locations(min_lat, max_lat, min_lon, max_lon)

# only choose the sites that are in the desired region
df = df[df['site'].isin(socal['site'])]
# add the lat and lon columns to the gps dataframe
df = df.join(socal.set_index('site'), on='site')
# query for the desired time interval
#df = df.query(start + '<= date_time < ' + end)
# convert from dask to pandas
df = df.compute()

# normalize displacement values at each station
#df = normalize(df)

min_displacement = min([min(df['e_ref']), min(df['n_ref']), min(df['v_ref'])])
max_displacement = max([max(df['e_ref']), max(df['n_ref']), max(df['v_ref'])])

times = df.date_time.unique()
count = 0

for time in times:
    count += 1

    time = datetime.datetime.utcfromtimestamp(time.tolist()/1e9)
    year = time.year
    month = time.month
    day = time.day
    hour = time.hour
    minute = time.minute
    dff = df.query('date_time == ' + 'datetime.datetime(' + str(year) + ',' + str(month) + ',' + str(day) + ',' + str(hour) + ',' + str(minute) + ',' + '0)')

    title = time

    # plot interpolation of data at that time
    grid_x, grid_y = np.mgrid[min_lat:max_lat:100j, min_lon:max_lon:200j]

    points_X = dff[['lon']]
    points_Y = dff[['lat']]
    points = dff[['lat', 'lon']].values

    grid_e = griddata(points, dff['e_ref'], (grid_x, grid_y), method='linear')
    grid_n = griddata(points, dff['n_ref'], (grid_x, grid_y), method='linear')
    grid_v = griddata(points, dff['v_ref'], (grid_x, grid_y), method='linear')
    # extrapolate the data outside the convex hull using the 'nearest' method
    grid_e_nearest = griddata(points, dff['e_ref'].values, (grid_x, grid_y), method='nearest')
    grid_n_nearest = griddata(points, dff['n_ref'].values, (grid_x, grid_y), method='nearest')
    grid_v_nearest = griddata(points, dff['v_ref'].values, (grid_x, grid_y), method='nearest')
    grid_e[np.isnan(grid_e)] = grid_e_nearest[np.isnan(grid_e)]
    grid_n[np.isnan(grid_n)] = grid_n_nearest[np.isnan(grid_n)]
    grid_v[np.isnan(grid_v)] = grid_v_nearest[np.isnan(grid_v)]


    fig, axs = plt.subplots(1, 3, figsize=(18.5, 6))
    #fig.tight_layout()
    plt.suptitle(title, fontsize=16)
    axs[0].imshow(grid_e.T, cmap='RdYlGn', vmin=min_displacement, vmax=max_displacement,
                  extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
    axs[0].plot(points_X, points_Y, "k.", ms=1)
    axs[0].set_title('East Displacement', weight='bold', fontsize=12)
    axs[0].set_ylabel('Latitude', weight='bold', fontsize=14)
    axs[1].imshow(grid_n.T, cmap='RdYlGn', vmin=min_displacement, vmax=max_displacement,
                  extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
    axs[1].plot(points_X, points_Y, "k.", ms=1)
    axs[1].set_title('North Displacement', weight='bold', fontsize=12)
    axs[1].set_xlabel('Longitude', weight='bold', fontsize=14)
    axs[2].imshow(grid_v.T, cmap='RdYlGn', vmin=min_displacement, vmax=max_displacement,
                  extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
    axs[2].plot(points_X, points_Y, "k.", ms=1)
    axs[2].set_title('Up Displacement', weight='bold', fontsize=12)
    cbar = fig.colorbar(axs[0].imshow(grid_e.T, cmap='RdYlGn', vmin=min_displacement, vmax=max_displacement,
                                      extent=(min_lon, max_lon, min_lat, max_lat), origin='lower'), ax=axs,
                        orientation='horizontal', label='Meters')
    plt.savefig('/Users/amyferrick/Downloads/2014_Slideshow/' + str(count))
    plt.close()
