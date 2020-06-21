import sqlite3
import numpy as np
import matplotlib.pyplot as plt


def make_arrays():
    connection = sqlite3.connect('data.db')
    cursor = connection.cursor()

    cursor.execute(
        'SELECT site,e_ref,n_ref,v_ref,date_time from gps WHERE site="BESI" AND date_time>="2013-02-28 00:00:00.000000" AND date_time<="2013-03-02 23:45:00.000000"')
    subset = cursor.fetchall()

    site = np.array([row[0] for row in subset])

    e_ref = np.array([row[1] for row in subset])

    n_ref = np.array([row[2] for row in subset])

    v_ref = np.array([row[3] for row in subset])

    time = np.array([row[4] for row in subset]).astype('datetime64')
    connection.close()

    return e_ref, n_ref, v_ref, time


values = make_arrays()
east_position = values[0]
north_position = values[1]
up_position = values[2]
time = values[3]


plt.figure(figsize=(20, 10))  # Create a figure twice as wide as it is high
plt.suptitle("Site: 'BESI'", fontsize=18, fontweight="bold")
plt.subplot(3, 1, 1)  # Create the first subpanel for the east displacement time series
plt.plot_date(time, east_position, "m.")  # Plot the east position time series
plt.xlabel("time", fontsize=12, fontweight="bold");
plt.ylabel("east position (units)", fontsize=14, fontweight="bold")  # Add descriptive axis labels
plt.xticks([min(time), max(time)], fontsize=12, fontweight='bold')
plt.subplot(3, 1, 2)  # Create the first subpanel for the east displacement time series
plt.plot(time, north_position, "m.")  # Plot the east position time series
plt.xlabel("time", fontsize=12, fontweight="bold");
plt.ylabel("north position (units)", fontsize=14, fontweight="bold")  # Add descriptive axis labels
# plt.xticks(['2013-02-28 00:00:00.000000', '2013-03-02 23:45:00.000000'],)
plt.subplot(3, 1, 3)  # Create the first subpanel for the east displacement time series
plt.plot(time, up_position, "m.")  # Plot the east position time series
plt.xlabel("time", fontsize=12, fontweight="bold");
plt.ylabel("up position (units)", fontsize=14, fontweight="bold")  # Add descriptive axis labels
plt.subplots_adjust(hspace=.3)
