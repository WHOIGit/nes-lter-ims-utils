# This Python Script reads the CTD Bottle file output from the API, i.e. https://nes-lter-data.whoi.edu/api/ctd/en608/bottles.csv
# and counts the occurances of each column header name in all bottle files for all cruises.
# The bottle files have been output from the API and are residing in a local directory.

import argparse
import os
from io import StringIO
import re
import pandas as pd
import csv
from collections import Counter

current_dir = os.getcwd()

buffer = StringIO()

def find_btl_files(url):
    matching_files = []
    if not os.path.exists(url):
        print(f"Directory '{url}' does not exist.")
        return matching_files

    # Compile a regex pattern for matching files
    pattern = r'.*ctd_bottles\.csv$'
    regex_pattern = re.compile(pattern)

    # Iterate through files in the directory
    for filename in os.listdir(url):
        if regex_pattern.match(filename):
            matching_files.append(os.path.join(url, filename))

    return matching_files

def count_hdr_names(file_path):
    print(f"Counting column header names in API output ctd_bottles.csv files for all cruises.")
    buffer.write(f"Counting column header names in API output ctd_bottles.csv files for all cruises.\n")

    files = find_btl_files(file_path)
    if len(files) == 0:
        print(f"There are no bottle files to read.")
        buffer.write(f"There are no bottle files to read.\n")
    else:
        print(f"Processing {len(files)} bottle files.")
        buffer.write(f"Processing {len(files)} bottle files.\n")
        count_ar_files = sum(1 for file_path in files if os.path.basename(file_path).startswith("ar"))
        print(f"There are {count_ar_files} Armstrong bottle files.")
        buffer.write(f"There are {count_ar_files} Armstrong bottle files.\n")
        count_en_files = sum(1 for file_path in files if os.path.basename(file_path).startswith("en"))
        print(f"There are {count_en_files} Endeavor bottle files.")
        buffer.write(f"There are {count_en_files} Endeavor bottle files.\n")
        count_at_files = sum(1 for file_path in files if os.path.basename(file_path).startswith("at"))
        print(f"There are {count_at_files} Atlantis bottle files.")
        buffer.write(f"There are {count_at_files} Atlantis bottle files.\n")
        count_hr_files = sum(1 for file_path in files if os.path.basename(file_path).startswith("hr"))
        print(f"There are {count_hr_files} Sharp bottle files.")
        buffer.write(f"There are {count_hr_files} Sharp bottle files.\n")
    
    header_name_counter = Counter() 

    for file in files:
        #print(f"Reading {file}")
        #buffer.write(f"Reading {file}\n")

        with open(file, 'r') as csv_file:
            csv_reader = csv.reader(csv_file)
                
            # Read the header row
            headers = next(csv_reader, None)
                
            # Update the header_name_counter with the counts from this file
            header_name_counter.update(set(headers))

    # Sort the results by count in descending order
    sorted_headers = sorted(header_name_counter.items(), key=lambda x: x[1], reverse=True)

    # Print the results
    for header_name, count in sorted_headers:
        print(f'Column header "{header_name}" appears {count} times in all files.')
        buffer.write(f'Column header "{header_name}" appears {count} times in all files.\n')
        
    buffer_content = buffer.getvalue()
    buffer.close()
    with open(file_path + "/" + "bottles_csv_col_names_count.txt", "w") as file:   # txt files don't support bold font
        file.write(buffer_content)

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Count the column header names for all bottles.csv files.')
    parser.add_argument('path', type=str, help='Path to Bottles.csv files')
    
    args = parser.parse_args()
    
    count_hdr_names(args.path)

if __name__ == '__main__':
    main()
