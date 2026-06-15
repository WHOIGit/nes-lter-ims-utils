# This Python Script fills in missing Cruise Underway PAR Sensor entries in the 1 minute .csv processed files

import argparse
import os
from io import StringIO
import pandas as pd
buffer = StringIO()
from glob import glob
import datetime as datetime
from pathlib import Path

DATE = 'date'
current_dir = os.getcwd()

def fix_underway(file_path):
    print(f"Fixing Underway file")    
    # read in the hrs2601 underway file
    file_path = Path(file_path)
    df = pd.read_csv(file_path / "hrs2601_underway_noMet_noPAR.csv")

    # read in and concatenate the PAR files
    pfs = []

    for file in glob(os.path.join(file_path, "Light Logger (PAR)", "BSI2026*.csv")):
        data = pd.read_csv(file, delimiter=",", skiprows=8, encoding="cp1252")
        pfs.append(data)

    pdf = pd.concat(pfs, ignore_index=True)

    # create an ISO 8601 datetime column
    date_obj = pd.to_datetime(
        pdf["Time"],
        errors="coerce",
        utc=True
    )

    pdf.insert(1, DATE, date_obj)

    # Merge "QSR - S/N 10367" underway data from the Light Logger Par data files based on time
    # into new column in the resulting dataFrame
    df['date'] = pd.to_datetime(df['date'], utc=True, errors='coerce')
    df = df.merge(pdf[['date', 'QSR - S/N 10367']], on='date', how='left')

    # write dataframe to underway file
    df.to_csv('hrs2601_underway.csv', index=False)


def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Fix Underway file.')
    parser.add_argument('path', type=str, help='Path to Underway file in the CTD directory')
    
    args = parser.parse_args()
    
    fix_underway(args.path)

if __name__ == '__main__':
    main()
