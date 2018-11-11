import datetime
import glob
import pandas as pd

OUTPUT_FILE_NAME = "unr_5min_gps"
YEAR_2000_OFFSET = datetime.datetime(2000, 1, 1, 12, 0)

def read_single_file(file_name):
    ''' Read in current file, rename columns, and create date_time column '''
    df = pd.read_csv(file_name, delim_whitespace=True)

    # Rename columns
    df.rename(columns={'___e-ref(m)': 'e_ref'}, inplace=True)
    df.rename(columns={'___n-ref(m)': 'n_ref'}, inplace=True)
    df.rename(columns={'___v-ref(m)': 'v_ref'}, inplace=True)
    df.rename(columns={'_e-mean(m)': 'e_mean'}, inplace=True)
    df.rename(columns={'_n-mean(m)': 'n_mean'}, inplace=True)
    df.rename(columns={'_v-mean(m)': 'v_mean'}, inplace=True)
    df.rename(columns={'sig_e(m)': 'sig_e'}, inplace=True)
    df.rename(columns={'sig_n(m)': 'sig_n'}, inplace=True)
    df.rename(columns={'sig_v(m)': 'sig_v'}, inplace=True)

    # Produce datetime objects and add as column.  This can probably be done as a list comprehension
    date_time = []
    for i in range(len(df)):
        date_time.append(YEAR_2000_OFFSET + datetime.timedelta(seconds=int(df["sec-J2000"].values[i])))
    df["date_time"] = date_time

    # Delete unneeded columns
    df = df.drop(columns=["sec-J2000", "__MJD", "year", "mm", "dd", "doy", "s-day"])

    return df


def main():
    ''' Get all valid filenames, read in each, build giant dataframe, and save to disk '''
    file_names = glob.glob('./*.kenv', recursive=True)

    # List comprehension is fast for catting all files, but has no error checking...gulp!
    dfs = [read_single_file(file_name) for file_name in file_names]
    df = pd.concat(dfs)

    # Store as and .pkl
    df.to_pickle(OUTPUT_FILE_NAME + ".pkl")
    
    # Save as feather...super fast but still alpha
    df.to_feather(OUTPUT_FILE_NAME + ".feather")
 

if __name__ == "__main__":
    main()
