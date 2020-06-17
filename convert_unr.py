import datetime
import glob
import pandas as pd
# from sqlalchemy import create_engine
from dask import dataframe as dd


KENV_ROOT_DIR = "/Users/meade/Desktop/data/5_min_gps_data"
# KENV_ROOT_DIR = "/home/meade/Desktop/data/5_min_gps/2013"

OUTPUT_FILE_NAME = "/Users/meade/Desktop/data/5_min_gps_data/unr_5min_gps"
YEAR_2000_OFFSET = datetime.datetime(2000, 1, 1, 12, 0)


def read_single_file(file_name):
    """ Read in current file, rename columns, and create date_time column """
    df = pd.read_csv(file_name, delim_whitespace=True)

    # Rename columns
    df.rename(columns={"___e-ref(m)": "e_ref"}, inplace=True)
    df.rename(columns={"___n-ref(m)": "n_ref"}, inplace=True)
    df.rename(columns={"___v-ref(m)": "v_ref"}, inplace=True)
    df.rename(columns={"_e-mean(m)": "e_mean"}, inplace=True)
    df.rename(columns={"_n-mean(m)": "n_mean"}, inplace=True)
    df.rename(columns={"_v-mean(m)": "v_mean"}, inplace=True)
    df.rename(columns={"sig_e(m)": "sig_e"}, inplace=True)
    df.rename(columns={"sig_n(m)": "sig_n"}, inplace=True)
    df.rename(columns={"sig_v(m)": "sig_v"}, inplace=True)

    # Produce datetime objects and add as column.  This can probably be done as a list comprehension
    date_time = []
    for i in range(len(df)):
        date_time.append(
            YEAR_2000_OFFSET
            + datetime.timedelta(seconds=int(df["sec-J2000"].values[i]))
        )
    df["date_time"] = date_time

    # Delete unneeded columns
    df = df.drop(columns=["sec-J2000", "__MJD", "year", "mm", "dd", "doy", "s-day"])
    return df


def write_to_disk(df):
    """ Write latest df to disk in multiple formats """
    # Store as and .pkl
    df.to_pickle(OUTPUT_FILE_NAME + ".pkl")

    # Save as feather...super fast but still alpha
    # df.to_feather(OUTPUT_FILE_NAME + ".feather")
    
    # You'll need to import sqlalchemy to use this.
    # engine = create_engine('sqlite:///data.db')
    
    # df.to_sql('gps', con=engine, schema=None, if_exists='replace', index=False, chunksize=None, dtype=None, method=None)

    # save as parquet file
    # note - each partition in the dask dataframe will be written to a separate file
    # you will need to install a parquet library (either fastparquet or pyarrow)
    # df.to_parquet(OUTPUT_FILE_NAME + ".parquet")

def main():
    """ Get all valid filenames, read in each, build giant dataframe, and save to disk """
    print("Globbing file names")
    file_names = glob.glob(KENV_ROOT_DIR + "/**/*.kenv", recursive=True)
    print("Done globbing file names")

    df_list = []
    for i in range(0, len(file_names)):
        try:
            print(str(i + 1) + " of " + str(len(file_names)) + " : " + file_names[i])
            df_list.append(
                read_single_file(file_names[i])
            )  # Building list because append cost is nearly free
        except:
            print(
                str(i + 1)
                + " of "
                + str(len(file_names))
                + " : "
                + file_names[i]
                + " : FAILED"
            )

    df = pd.concat(df_list)  # Now one big concat instead of millions of small ones
    df.reset_index(inplace=True)
    
    # to convert to dask dataframe:
    # number of partitions should be 1-2x the number of cores in your CPU
    # df = dd.from_pandas(df, npartitions=8)
    write_to_disk(df)


if __name__ == "__main__":
    main()
