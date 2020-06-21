import datetime
import pandas as pd
import numpy as np
from dask import dataframe as dd
import matplotlib.pyplot as plt
from pandas.plotting import register_matplotlib_converters
register_matplotlib_converters()

#load in the dask dataframe
df = dd.read_parquet("/Users/amyferrick/Downloads/5_min_gps_subset/unr_5min_gps" + ".parquet")

def query_data(df, site_ID, start_time, end_time):
    # returns the position component arrays for a specific site and time interval
    # start time and end time must be date time objects

    one_site = df.query('site == ' + site_ID)
    in_interval = one_site.query(start_time + ' <= date_time < ' + end_time)

    time = in_interval.date_time.compute().values
    e = in_interval.e_ref.compute().values
    n = in_interval.n_ref.compute().values
    v = in_interval.v_ref.compute().values
    return time, e, n, v

site_ID = '"LHAW"' # fill in site ID here (the double quotation is necessary)
siteID = 'LHAW' # with single quotes, need this for plotting later
start_time = 'datetime.datetime(2013, 2, 28, 12)'# fill in start of time interval
end_time = 'datetime.datetime(2013, 3, 3, 12)' # fill in end of time interval

time, e, n, v = query_data(df, site_ID, start_time, end_time)


# plot the three panel time series
plt.figure(figsize=(20,10))
plt.suptitle("Site: "+site_ID, weight='bold')
plt.subplot(3, 1, 1)
plt.plot(time, e, "r.", ms=1)
plt.xlabel("time", weight='bold')
plt.ylabel("east position (units)", weight = 'bold')
plt.subplot(3, 1, 2)
plt.plot(time, n, "r.", ms=1)
plt.xlabel("time", weight='bold')
plt.ylabel("north position (units)", weight='bold')
plt.subplot(3, 1, 3)
plt.plot(time, v, "r.", ms=1)
plt.xlabel("time", weight='bold')
plt.ylabel("up position (units)", weight='bold')
plt.savefig("/Users/amyferrick/Downloads/5_min_gps_subset/" + siteID + ".png")
plt.show()
