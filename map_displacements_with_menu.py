import datetime
import pandas as pd
import numpy as np
from dask import dataframe as dd
import matplotlib.pyplot as plt
from scipy.interpolate import griddata
from datetime import timedelta, date


def daterange(start_date, end_date):
    for n in range(int((end_date - start_date).days)):
        yield start_date + timedelta(n)

def query_locations(mini_lat, maxi_lat, mini_lon, maxi_lon):
    # returns only the stations in a given area, based on limits of latitude and longitude
    region = stations.query(str(mini_lat) + ' < lat < ' + str(maxi_lat)).query(str(mini_lon) + ' < lon < ' + str(maxi_lon))
    return region


def find_limits(df):
    min_displacement = min([min(df['e_ref']), min(df['n_ref']), min(df['v_ref'])])
    max_displacement = max([max(df['e_ref']), max(df['n_ref']), max(df['v_ref'])])
    return min_displacement, max_displacement


def normalize_whole(df):
    df.e_ref = df.e_ref - (df.e_ref.mean())
    df.v_ref = df.v_ref - (df.v_ref.mean())
    df.n_ref = df.n_ref - (df.n_ref.mean())
    return df


def normalize_sites(df):
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
    return df

def create_images(dfm, sy, sm, sd, ey, em, ed):
    print('STARTING!!!')
    min_displacement, max_displacement = find_limits(df)


    # Making a list of dates to go over
    date_list = []
    start_date = date(sy, sm, sd)
    end_date = date(ey, em, ed)
    for single_date in daterange(start_date, end_date):
        date_list.append(single_date.strftime(("%Y,%#m,%#d")))
        print(single_date.strftime("%Y,%m,%d"))

    print("Finding Times")
    times = df.date_time.unique()

    # Starting hour of loop
    count = 0
    print("Starting the iteration")
    for time in times:
        count += 1
        print(count)

        time = datetime.datetime.utcfromtimestamp(time.tolist() / 1e9)
        year = time.year
        month = time.month
        day = time.day
        hour = time.hour
        minute = time.minute
        dff = df.query(
            'date_time == ' + 'datetime.datetime(' + str(year) + ',' + str(month) + ',' + str(day) + ',' + str(
                hour) + ',' + str(minute) + ',' + '0)')

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

        # fig.tight_layout()
        plt.suptitle(title, fontsize=16)
        axs[0].imshow(grid_e.T, cmap='Spectral', vmin=min_displacement, vmax=max_displacement,
                      extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
        axs[0].plot(points_X, points_Y, "k.", ms=1)
        axs[0].set_title('East Displacement', weight='bold', fontsize=12)
        axs[0].set_ylabel('Latitude', weight='bold', fontsize=14)
        axs[1].imshow(grid_n.T, cmap='Spectral', vmin=min_displacement, vmax=max_displacement,
                      extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
        axs[1].plot(points_X, points_Y, "k.", ms=1)
        axs[1].set_title('North Displacement', weight='bold', fontsize=12)
        axs[1].set_xlabel('Longitude', weight='bold', fontsize=14)
        axs[2].imshow(grid_v.T, cmap='Spectral', vmin=min_displacement, vmax=max_displacement,
                      extent=(min_lon, max_lon, min_lat, max_lat), origin='lower')
        axs[2].plot(points_X, points_Y, "k.", ms=1)
        axs[2].set_title('Up Displacement', weight='bold', fontsize=12)
        cbar = fig.colorbar(axs[0].imshow(grid_e.T, cmap='Spectral', vmin=min_displacement, vmax=max_displacement,
                                          extent=(min_lon, max_lon, min_lat, max_lat), origin='lower'), ax=axs,
                            orientation='horizontal', label='Meters')
        plt.savefig('E:/Pictures/2016_Slideshow/' + str(count))
        plt.close()
    print('DONE!!!')


def change_the_timeframe(minimum_lat, maximum_lat, minimum_lon, maximum_lon):
    a, b, c = input('Enter the START Year(YYYY), Month(m), Day(d)').split()
    w, x, y = input('Enter the END Year(YYYY), Month(m), Day(d)').split()
    beginning = 'datetime.datetime(' + str(a) + ',' + str(b) + ',' + str(c) + ', 0 , 0, 0)'
    final = 'datetime.datetime(' + str(w) + ',' + str(x) + ',' + str(y) + ', 0 , 0, 0)'

    # read in the gps data
    df_nt = dd.read_parquet('E:/5_minute_gps/rapid_gps-master/2015_14days.parquet')
    socal = query_locations(minimum_lat, maximum_lat, minimum_lon, maximum_lon)
    # only choose the sites that are in the desired region
    df_nt = df_nt[df_nt['site'].isin(socal['site'])]
    # add the lat and lon columns to the gps dataframe
    df_nt = df_nt.join(socal.set_index('site'), on='site')
    # query for the desired time interval
    df_nt = df_nt.query(beginning + '<= date_time < ' + final)
    # convert from dask to pandas
    df_nt = df_nt.compute()
    return df_nt, beginning, final


def select_new_area(beginning, final):
    minlat, maxlat = input('Assign the Latitude min and max: ').split()
    minlon, maxlon = input('Assign the Longitude min and max: ').split()

    # read in the gps data
    df_na = dd.read_parquet('E:/5_minute_gps/rapid_gps-master/2015_14days.parquet')

    socal = query_locations(minlat, maxlat, minlon, maxlon)

    # only choose the sites that are in the desired region
    df_na = df_na[df_na['site'].isin(socal['site'])]
    # add the lat and lon columns to the gps dataframe
    df_na = df_na.join(socal.set_index('site'), on='site')
    # query for the desired time interval
    df_na = df_na.query(beginning + '<= date_time < ' + final)
    # convert from dask to pandas
    df_na = df_na.compute()
    return df_na, minlat, maxlat, minlon, maxlon


def retrieve_coordinates(minimum_lat, maximum_lat, minimum_lon, maximum_lon):
    print('min_lat: ' + str(minimum_lat))
    print('max_lat: ' + str(maximum_lat))
    print('min_lon: ' + str(minimum_lon))
    print('max_lon: ' + str(maximum_lon))


def retrieve_timeframe(beginning, final):
    print("Start = " + beginning)
    print("End = " + final)


print('Loading Defaults')

# read in the station info
stations = pd.read_csv('locations.txt', sep='  ', engine='python', names=['site', 'lat', 'lon', 'height'])

lon = stations['lon']
lon = lon.where(lon > -180, other=lon+360) #a lot of the longitudes are less than -180. this makes them between -180 and +180
stations.drop(columns=['lon'])
stations['lon'] = lon

# assign the desired time interval
start = 'datetime.datetime(2015, 1, 1, 0, 0, 0)'
end = 'datetime.datetime(2015, 1, 2, 0, 0, 0)'
start_year = 2015
start_month = 1
start_day = 1
end_year = 2015
end_month = 1
end_day = 2


# assign the desired limits for latitude and longitude
min_lat = 32
max_lat = 36
min_lon = -120
max_lon = -114

# read in the gps data
df = dd.read_parquet('E:/5_minute_gps/rapid_gps-master/2015_14days.parquet')

socal = query_locations(min_lat, max_lat, min_lon, max_lon)

# only choose the sites that are in the desired region
df = df[df['site'].isin(socal['site'])]
# add the lat and lon columns to the gps dataframe
df = df.join(socal.set_index('site'), on='site')
# query for the desired time interval
df = df.query(start + '<= date_time < ' + end)
# convert from dask to pandas
df = df.compute()


user_options = {
    "w": normalize_whole,
    "s": normalize_sites,
    "c": create_images,
    "l": retrieve_coordinates,
    "t": retrieve_timeframe,
    "ch": change_the_timeframe,
    "se": select_new_area
}

MENU_PROMPT = "\nEnter 'w' normalize based on the mean of the whole,"\
              "\n's' Normalize each site by its mean," \
              "\n'l' Retrieve the latitude and longitude values,"\
              "\n'c' create the images," \
              "\n't' Return Timeframe, or"\
              "\n'q' to quit: "


def menu(dfm, minimum_lat, maximum_lat, minimum_lon, maximum_lon, beginning, final, sy, sm, sd, ey, em, ed):
    selection = input(MENU_PROMPT)
    while selection != 'q':
        if selection in user_options:
            selected_function = user_options[selection]
            if selection == 'w' or selection == 's':
                selected_function(dfm)
            elif selection == 'ch':
                dfm, beginning, final = selected_function(minimum_lat, maximum_lat, minimum_lon, maximum_lon)
            elif selection == 'se':
                dfm, minimum_lat, maximum_lat, minimum_lon, maximum_lon = selected_function(beginning, final)
            elif selection == 'l':
                selected_function(minimum_lat, maximum_lat, minimum_lon, maximum_lon)
            elif selection == 't':
                selected_function(beginning, final)
            elif selection == 'c':
                selected_function(dfm, sy, sm, sd, ey, em, ed)

        else:
            print('Unknown command. Please try again.')

        selection = input(MENU_PROMPT)


if __name__ == "__main__":
    menu(df, min_lat, max_lat, min_lon, max_lon, start, end, start_year, start_month, start_day, end_year, end_month, end_day)
