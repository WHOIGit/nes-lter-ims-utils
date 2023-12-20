# Create Underway file from raw Sharp Cruise (HRS2303) SMS Data 
# Download your SMS Data .txt files and Sharp Header Workbook.xls to 
# your local folder and provide the path to your folder.
import argparse
from glob import glob
import os
import pandas as pd
import datetime as dt

START_TIME = '2023-04-29 03:29:51'
DATE = 'date'

def clean_data(path):
    # read in and concatenate the 10-sec SMS .txt files
    dfs = []
    print("Path:", path)
    for file in glob(os.path.join(path, '*.txt')):
        data = pd.read_csv(file, delimiter=',', header=None) # Specify header=None to treat all rows as data
        dfs.append(data)
    df = pd.concat(dfs, ignore_index=True)

    # delete empty columns
    df = df.dropna(axis=1, how='all')

    # read first two rows as column headers from Sharp Header Workbook.xls, SMS tab
    df_headers = pd.read_excel('Sharp Header Workbook.xls', header=None, nrows=2, sheet_name='SMS')
    # drop the last column with nan values
    df_headers = df_headers.iloc[:,:-1]
    # combine first column header name with underscore, followed by second column header name
    new_header = df_headers.iloc[0] + '_' + df_headers.iloc[1].astype(str)
    df.columns = new_header

    # drop Time LMT column
    df = df.drop('Time_LMT', axis=1)
    # drop both sets of Latitude Deg Min, Longitude Deg Min
    df = df.drop('Latitude_Deg Min', axis=1)
    df = df.drop('Lattitude _Deg Min', axis=1)  # misspelled & extra space
    df = df.drop('Longitude_Deg Min', axis=1)
    df = df.drop('Longitude_DegMin', axis=1)
    # drop both Suffix columns
    df = df.drop('Suffix_nan', axis=1)
    # drop Depth Feet and Depth Fathom
    df = df.drop('Depth_Feet', axis=1)
    df = df.drop('Depth_Fathom', axis=1)
    # drop the Counter since we already have it in Date Column
    df = df.drop('Counter_Seconds', axis=1)
    # drop the Cruise name since having it is not consistent with other underway files
    df = df.drop('Cruise Name_Text', axis=1)
    # The SMS data computer time is the leftmost Time GMT - use this one and drop the other duplicate Time GMT
    df = df.loc[:,~df.columns.duplicated()].copy()

    # create an ISO 8601 datetime column (this is the “date” column in product)
    date_series = df['Date_GMT'].copy()
    date_obj = date_series.apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y'))
    formatted_date = date_obj.dt.strftime('%Y-%m-%d')
    datetime = formatted_date + ' ' + df[('Time_GMT')].copy()
    df.insert(1, DATE, pd.to_datetime(datetime, utc=True, format="ISO8601"))
    # drop original Date and Time GMT column since we have the date & time in the new date field
    df = df.drop('Date_GMT', axis=1)
    df = df.drop('Time_GMT', axis=1)
    # sort chronologically by ISO 8601 datetime
    df = df.sort_values(by=DATE)

    # exclude entries before the start time 
    start_time = pd.to_datetime(START_TIME, utc=True, format="ISO8601")
    # Filter and drop rows where the column values are less than the start time
    df = df[df[DATE] >= start_time]
    # decimate the datetime to 1-min frequency by selecting every 6th row
    df.reset_index(drop=True, inplace=True)
    df = df[::6].reset_index(drop=True)

    # change decimal longitude (column Longitude Deg) to (-)
    df['Longitude_Deg'] = -df['Longitude_Deg']

    # INSERT EQUATION FROM SHIP TECHS - equation could not be provided
    # add new column for fluorometer calibration
    # calibration = df['Fluorometer_Turner Raw'] * 10
    # df.insert(df.columns.get_loc('Fluorometer_Turner Raw') + 1, 'fluorometer_cal', calibration)

    # read in and concatenate the PAR files
    pfs = []
    for file in glob(os.path.join(path, 'BSI2023*.csv')):
        data = pd.read_csv(file, delimiter=',', skiprows=9, encoding='cp1252') # Specify header=None to treat all rows as data
        pfs.append(data)
    pdf = pd.concat(pfs, ignore_index=True)
    
    # create an ISO 8601 datetime column (this is the “Time” column in product)
    date_series = pdf['Time'].copy()
    # invalid data in BSI20230502_154704.csv line 5931
    # date_obj = date_series.apply(lambda x: dt.datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p'))
 
    def parse_date(x):
        try:
            return dt.datetime.strptime(x, '%m/%d/%Y %I:%M:%S %p')
        except ValueError:
            return pd.NaT  # Return a "Not a Time" value for invalid dates
    datetime = date_series.apply(parse_date)

    pdf.insert(1, DATE, pd.to_datetime(datetime, utc=True, format="ISO8601"))
    
    # Merge "QSR - S/N 10367" underway data from the Light Logger Par data files based on time
    # into new column in the resulting dataFrame
    df = df.merge(pdf[['date', 'QSR - S/N 10367']], on='date', how='left')

    # write dataframe to underway file
    df.to_csv('HRS2303_Data60Sec_200429-0000.csv', index=False)

def main():
    parser = argparse.ArgumentParser(description='Clean Sharp Underway Cruise Data for REST API.')
    parser.add_argument('path', type=str, help='Path to data files')

    args = parser.parse_args()
    clean_data(args.path)

if __name__ == '__main__':
    main()
