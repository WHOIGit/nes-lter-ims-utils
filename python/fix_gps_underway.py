# This Python Script fills in missing Cruise Underway GPS lat, long entries in the 1 minute .csv processed files by obtaining the correct entries from 
# the Underway GPS files in the same directory.
# This script also fills in the salinity, temp, fluorometer, and flow rate if they are missing. These values are obtained from SSW files.

import argparse
import os
from io import StringIO
import re
import pandas as pd
buffer = StringIO()
from datetime import datetime
import math

current_dir = os.getcwd()

def find_gps_file(url, min_file):
    matching_file = ''
    pattern = r'[a-zA-Z]+(\d{6})_\d+\.csv'
    match = re.search(pattern, min_file)
    # extract date from 1min file name
    extracted_value = match.group(1)
    
    gps_regex_pattern = rf'.*GPS(\d+)_{re.escape(extracted_value)}_0000\.csv'

    # Iterate through files in the directory
    for filename in os.listdir(url):
        if re.match(gps_regex_pattern, filename):
            matching_file = (os.path.join(url + '\\', filename))

    return matching_file

def find_ssw_file(url, min_file):
    matching_file = ''
    pattern = r'[a-zA-Z]+(\d{6})_\d+\.csv'
    match = re.search(pattern, min_file)
    # extract date from 1min file name
    extracted_value = match.group(1)
    
    gps_regex_pattern = rf'.*SSW(\d+)_{re.escape(extracted_value)}_0000\.csv'

    # Iterate through files in the directory
    for filename in os.listdir(url):
        if re.match(gps_regex_pattern, filename):
            matching_file = (os.path.join(url + '\\', filename))

    return matching_file

def find_1min_files(url):
    matching_files = []
    if not os.path.exists(url):
        print(f"Directory '{url}' does not exist.")
        return matching_files

    # Compile a regex pattern for matching files
    pattern = r'^[a-zA-Z]+\d+_0000\.csv$'
    regex_pattern = re.compile(pattern)

    # Iterate through files in the directory
    for filename in os.listdir(url):
        if regex_pattern.match(filename):
            matching_files.append(os.path.join(filename))

    return matching_files

def find_row_by_time(file, time_gmt, invalid_time_strings):
    df = pd.read_csv(file, delimiter=',', header = 1)  # second row is header row)

    # Strip leading and trailing whitespaces from the target time string
    time_gmt_str = time_gmt.strip()
    target_time = datetime.strptime(time_gmt_str, '%H:%M:%S.%f')
    
    # Calculate the time difference for each row
    def calculate_time_difference(x):
        try:
            time_value = datetime.strptime(x.strip(), '%H:%M:%S.%f')
            return abs((target_time - time_value).total_seconds())
        except:
            # Handle the case where the time data is invalid
            if (x, file) not in invalid_time_strings:
                invalid_time_strings.append((x, file))
            return float('inf')  # Set a default value

    df['TIME_DIFF'] = df[' TIME_GMT'].apply(calculate_time_difference)

    # Find the row with the minimum time difference, excluding NaN values
    nearest_row = df.loc[df['TIME_DIFF'].idxmin()]
    
    return nearest_row

def fix_gps(file_path):
    invalid_time_strings = []
    print(f"Fixing GPS in 1 min .csv Underway files")    

    files = find_1min_files(file_path)
    if len(files) == 0:
        print(f"There are no 1 min files to check for this cruise.")
    
    for file in files:
        nan_found = False
        print(f"Fixing {file}")
        df1 = pd.read_csv(os.path.join(file_path,file), header = 1)  # second row is header row

        # find corresponding GPS file
        gps_file = find_gps_file(file_path, file)
            
        # find corresponding Science SeaWater (SSW) file
        ssw_file = find_ssw_file(file_path, file)
            
        df_modified = df1.copy()
        
        for index, row in df1.iloc[:].iterrows():
            time_gmt = row[' TIME_GMT']
            
            if row[' Dec_LAT'] == ' NAN' or row[' Dec_LON'] == ' NAN':   #note space in column header name and NAN value
                #find row by nearest time in gps file
                nearest_row = find_row_by_time(gps_file, time_gmt, invalid_time_strings)
                if nearest_row[' DPS112_LAT'] == ' NAN' or nearest_row[' DPS112_LON'] == ' NAN':
                    print(f"ERROR: GPS file lat, lon values are NAN. Cannot fix Underway file.")
                else:
                    df_modified.at[index, ' Dec_LAT'] = nearest_row[' DPS112_LAT']
                    df_modified.at[index, ' Dec_LON'] = nearest_row[' DPS112_LON']
                    nan_found = True
                    
            if math.isnan(row[' SBE45S']) or math.isnan(row[' SBE48T']) or math.isnan(row[' FLR']) or math.isnan(row[' FLOW']):
                #find row by nearest time in ssw file
                nearest_row = find_row_by_time(ssw_file, time_gmt, invalid_time_strings)
                #get salinity, temp, fluorometer, and flow rate in ssw file 
                if nearest_row[' SBE45S'] == '':
                    print(f"ERROR: SSW File salinity value is NAN. Cannot fix Underway file.")
                else:
                    df_modified.at[index, ' SBE45S'] = nearest_row[' SBE45S']
                    nan_found = True
                if  nearest_row[' SBE48T'] == '':   
                    print(f"ERROR: SSW File temperature value is NAN. Cannot fix Underway file.")
                else:
                    df_modified.at[index, ' SBE48T'] = nearest_row[' SBE48T']
                    nan_found = True
                if nearest_row[' FLR'] == '':
                    print(f"ERROR: SSW File fluormeter value is NAN. Cannot fix Underway file.")
                else:
                    df_modified.at[index, ' FLR'] = nearest_row[' FLR']
                    nan_found = True
                if nearest_row[' FLOW'] == '':
                    print(f"ERROR: SSW File flow rate value is NAN. Cannot fix Underway file.")
                else:
                    df_modified.at[index, ' FLOW'] = nearest_row[' FLOW']
                    nan_found = True
        
        if nan_found:
            # Write the modified DataFrame to a new file 
            base_filename, extension = os.path.splitext(file)
            output_filename = f"{base_filename}_new{extension}"
            df_modified.to_csv(os.path.join(file_path,output_filename), index=False)
        
            # Read the first line of the source CSV file as text ('WHOI SSSG dsLogCsv' does not conform to df with columns)
            first_line_df = pd.read_csv(os.path.join(file_path,file), nrows=1, header=None)
            # Create a sample DataFrame representing the rest of the data in the modified file
            rest_of_data_df = pd.read_csv(os.path.join(file_path,output_filename), header=None)
            # Concatenate the first line with the rest of the data
            result_df = pd.concat([first_line_df, rest_of_data_df], ignore_index=True)

            # Write the result DataFrame to a new CSV file
            result_df.to_csv(os.path.join(file_path,output_filename), index=False, header=False)    

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Fix GPS on 1 min Underway files (i.e. AR211028_0000.csv).')
    parser.add_argument('path', type=str, help='Path to Underway files in the proc directory')
    
    args = parser.parse_args()
    
    fix_gps(args.path)

if __name__ == '__main__':
    main()
