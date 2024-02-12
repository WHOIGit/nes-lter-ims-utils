# This Python Script fixes the AR70b 1 minute Underway files
# AR221120_000.csv - 
#      delete erroneous rows after 14:54:00:800
#      add missing rows 14:55 - 19:30
#      concatonate rows from AR221120_1931.csv (this file should not exist)
# After this script runs, run fix_gps_underway.py to populate lat, lon from the gps file

import argparse
import os
from io import StringIO
import re
import pandas as pd
buffer = StringIO()
from datetime import datetime, timedelta

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

def find_latlon_by_time(gps_file, time_gmt, invalid_time_strings):
    df = pd.read_csv(gps_file, delimiter=',', header = 1)  # second row is header row)

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
            if (x, gps_file) not in invalid_time_strings:
                invalid_time_strings.append((x, gps_file))
            return float('inf')  # Set a default value

    df['TIME_DIFF'] = df[' TIME_GMT'].apply(calculate_time_difference)

    # Find the row with the minimum time difference, excluding NaN values
    nearest_row = df.loc[df['TIME_DIFF'].idxmin()]
    
    #print(f'using nearest: {nearest_row[" TIME_GMT"]}')
    
    # Return the lat/lon at that row
    nearest_lat = nearest_row[' DPS112_LAT']
    nearest_lon = nearest_row[' DPS112_LON']
    
    return nearest_lat, nearest_lon

def fix_gps(file_path):
    invalid_time_strings = []
    file = 'AR221120_0000.csv'
    file_1931 = 'AR221120_1931.csv'
    print(f"Fixing AR70b AR221120_000.csv Underway file")    
    
    print(f"Fixing {file}")
    df = pd.read_csv(os.path.join(file_path,file), header = 1)  # second row is header row
    
    # Find the index of the row with TIME_GMT equal to '14:54:00.800'
    index_to_keep = df[df[' TIME_GMT'] == ' 14:51:00.800'].index[0]   #space required

    # Keep only the rows before this index
    df1 = df.loc[:index_to_keep]
    
    # Create a list of datetime values for the new rows
    new_rows_data = []
    start_time = pd.to_datetime('14:52:00.800', format='%H:%M:%S.%f').time()
    end_time = pd.to_datetime('19:30:00.800', format='%H:%M:%S.%f').time()

    current_time = start_time
    while current_time <= end_time:
        new_rows_data.append(['2022/11/20', current_time, ' NAN', ' NAN'])
        current_time = (datetime.combine(datetime.min, current_time) + timedelta(minutes=1)).time()

    for i in range(len(new_rows_data)):
        new_rows_data[i][1] = ' ' + new_rows_data[i][1].strftime('%H:%M:%S.%f')[:-3]  # Add space before current_time
        
    # Convert the list of new rows to a DataFrame
    new_rows_df = pd.DataFrame(new_rows_data, columns=['DATE_GMT', ' TIME_GMT', ' Dec_LAT', ' Dec_LON'])

    # Concatenate the original DataFrame with the new rows DataFrame
    df_concat = pd.concat([df1, new_rows_df], ignore_index=True)
    
    # read in AR221120_1931.csv
    df_1931 = pd.read_csv(os.path.join(file_path,file_1931), header = 1)  # second row is header row
    
    # Concatenate new df with rows from AR221120_1931.csv
    df_result = pd.concat([df_concat, df_1931], ignore_index=True)

    # Write the modified DataFrame to a new file 
    base_filename, extension = os.path.splitext(file)
    output_filename = f"{base_filename}_new{extension}"
    df_result.to_csv(os.path.join(file_path,output_filename), index=False)
        
    # Add the first line of the source CSV file as text ('WHOI SSSG dsLogCsv' does not conform to df with columns)
    first_line_df = pd.read_csv(os.path.join(file_path,file), nrows=1, header=None)
    # Read in the rest of the data in the file
    rest_of_data_df = pd.read_csv(os.path.join(file_path,output_filename), header=None)
    # Concatenate the first line with the rest of the data
    result_df = pd.concat([first_line_df, rest_of_data_df], ignore_index=True)

    # Write the result DataFrame to a new CSV file
    result_df.to_csv(os.path.join(file_path,output_filename), index=False, header=False)    

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Fix 1 min Underway file for AR70b (i.e. AR211020_0000.csv).')
    parser.add_argument('path', type=str, help='Path to Underway files in the proc directory')
    
    args = parser.parse_args()
    
    fix_gps(args.path)

if __name__ == '__main__':
    main()
