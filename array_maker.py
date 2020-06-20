import sqlite3
import numpy as np

def make_arrays():
    connection = sqlite3.connect('data.db')
    cursor = connection.cursor()

    cursor.execute('SELECT site,e_ref,n_ref,v_ref,date_time from gps WHERE site="BESI" AND date_time>="2013-02-28 00:00:00.000000" AND date_time<="2013-03-02 23:45:00.000000"')
    subset = cursor.fetchall()
  

    site = np.array([row[0] for row in subset])
    
    
    e_ref = np.array([row[1] for row in subset])
  

    n_ref = np.array([row[2] for row in subset])
  

    v_ref = np.array([row[3] for row in subset])
  

    time = np.array([row[4] for row in subset])
    connection.close()

    return e_ref, n_ref, v_ref, time
