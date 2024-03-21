# Protocol for reviewing processed CTD header data prior to upload to RDS for NES-LTER REST API
# Check header files for:
#     modulo errors from cable problems
#     includes sensors that you care about
#     min, max range checks for sensors 

import argparse
import os
from io import StringIO
import re
from glob import glob

buffer = StringIO()

current_dir = os.getcwd()
errors_found = False

def find_hdr_files(url):
    global errors_found
    matching_files = []
    for file in glob(os.path.join(url, '*.hdr')):
        # get base filename
        filename = os.path.splitext(os.path.basename(file))[0]
        if ('_u' not in filename and
            not (filename.startswith("dar") or filename.startswith("uar")) and
            '_up' not in filename and
            '_down' not in filename):
            matching_files.append(file)
    return matching_files

def check_nmea(file_path):
    global errors_found
    latitude = ''
    longitude = ''

    try:
        with open(file_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Search for the string in each line
            for line_number, line in enumerate(lines, start=1):
                pattern = r'NMEA Latitude = (\d+) (\d+\.\d+) (\w)'
                # Use re.search to find the match
                match = re.search(pattern, line)
                # Extract the matched value
                if match:
                    latitude = match.group(1)
                    dir = match.group(3)
                    if dir == "S":
                        latitude = -int(latitude)
                    if int(latitude) < 37 or int(latitude) > 43:
                       print(f"ERROR: NMEA Latitude {latitude} {dir} outside of range 37 to 43")
                       buffer.write(f"<strong>ERROR:</strong> NMEA Latitude {latitude} outside of range 37 to 43<br>")
                       errors_found = True
                       
                pattern = r'NMEA Longitude = (\d+) (\d+\.\d+) (\w)'
                # Use re.search to find the match
                match = re.search(pattern, line)
                # Extract the matched value
                if match:
                    longitude = match.group(1)
                    dir = match.group(3)
                    if dir == "W":
                        longitude = -int(longitude)
                    if int(longitude) > -67 or int(longitude) < -72:
                       print(f"ERROR: NMEA Longitude {longitude} outside of range -67 to -72")
                       buffer.write(f"<strong>ERROR:</strong> NMEA Longitude {longitude} outside of range -67 to -72<br>")
                       errors_found = True
                       
            if latitude == '':
                print(f"ERROR: NMEA Latitude not found")
                buffer.write(f"<strong>ERROR:</strong> NMEA Latitude not found<br>")
                errors_found = True
            
                
            if longitude == '':
                print(f"ERROR: NMEA Longitude not found")
                buffer.write(f"<strong>ERROR:</strong> NMEA Longitude not found<br>")
                errors_found = True
 
                    
    except FileNotFoundError:
        print(f"File not found: {file}")
        buffer.write(f"File not found: {file}<br>")

def check_modulo(file_path):

    try:
        with open(file_path, 'r') as file:
            # Read all lines from the file
            lines = file.readlines()

            # Search for the string in each line
            for line_number, line in enumerate(lines, start=1):
                if "modError" in line:
                    print(f"ERROR: modError found in file")
                    buffer.write(f"<strong>ERROR:</strong> modError found in file<br>")
                    errors_found = True
                    
    except FileNotFoundError:
        print(f"File not found: {file}")
        buffer.write(f"File not found: {file}<br>")
        
def summarize_errors():
    error_files = {}

    # Split the buffer content into sections for each file
    sections = buffer.getvalue().split("Checking ")

    for section in sections[1:]:  # Skip the first empty section
        lines = section.strip().split('<br>')
        file_path = lines[0].strip()
        
        for line in lines[1:]:
            if "ERROR:" in line or "NOTE:" in line or "Duplicate Variable Names Found" in line:
               error_files.setdefault(line, set()).add(file_path)

    return error_files
        
def check_duplicates(file):
    seen = set()
    duplicates = []
    
    with open(file, 'r') as file:
        file.seek(0)
            
        # Define the variable pattern name in the file
        pattern = re.compile(r'# name \d+ = (\w+):')
            
        # Find all the variable names
        for line in file:
            # Try to match the pattern in the current line
            match = pattern.match(line)
            if match:
                # Extract name from the matched pattern
                name = match.group(1)
                if name in seen:
                    duplicates.append(name)
                else:
                   seen.add(name)

    if duplicates:
        print("Duplicate Variable Names Found:")
        buffer.write(f"Duplicate Variable Names Found:")
        for duplicate in duplicates:
            print(duplicate)
            buffer.write(f" {duplicate}, ")
    else:
        print("No Duplicate Variable Names Found.")
        buffer.write(f"No Duplicate Variable Names Found.")
        
    buffer.write(f"<br>")

    
def check_sensor_data(file):
    global errors_found
    sensor_limits = {
    "prDM": {"min": 0, "max": 510},
    "t090C": {"min": -5, "max": 35},
    "t190C": {"min": -5, "max": 35},
    "c0S/m": {"min": 0, "max": 0}, 
    "c1S/m": {"min": 0, "max": 0}, 
    "CStarAt0": {"min": 0, "max": 100},
    "CStarTr0": {"min": 0, "max": 100},
    "flECO-AFL": {"min": -.5, "max": 10},
    "pumps": {"min": 1, "max": 1},
    "latitude": {"min": 37, "max": 43},
    "longitude": {"min": -72, "max": -67},
    "sal00": {"min": 27, "max": 37},
    "sal11": {"min": 27, "max": 37},
    "sbeox0V": {"min": 0, "max": 0},
    "sbeox1V": {"min": 0, "max": 0},
}

    sensors_info = []

    # Open and read the hdr file
    with open(file, 'r') as file:

        for idx in sensor_limits:
            found = False
            file.seek(0)
            
            # Define the sensor pattern in hdr file
            pattern = re.compile(r'# name (\d+) = ' + re.escape(idx) + r': (\w+)')
            
            # Iterate through each line in the file
            for line in file:
                # Try to match the pattern in the current line
                match = pattern.match(line)
                if match:
                    # Extract information from the matched pattern
                    number = match.group(1)
                    full_name = match.group(2)
                
                    sensor_info = {"value": idx, "number": number, "full_name": full_name}
    
                    # Append the dictionary to sensors_info
                    sensors_info.append(sensor_info)
                    found = True      # don't break out of loop - needed to find duplicates
                        
            if not found:
                if idx == 'latitude' or idx == 'longitude':
                    print(f"NOTE: Sensor {idx} is not a processed variable")
                    buffer.write(f"NOTE: Sensor {idx} is not a processed variable<br>")
                else:
                    print(f"ERROR: Sensor {idx} variable not output")
                    buffer.write(f"<strong>ERROR:</strong> Sensor {idx} variable not output<br>")
                errors_found = True
                                  
        # range check the sensors
        for item in sensors_info:
            found = False
            file.seek(0)
            if item['value'] not in ['c0S/m', 'c1S/m', 'sbeox0V', 'sbeox1V']:
                pattern = re.compile(r'# span ' + re.escape(item['number']) + r' =\s+(-?\d+(\.\d+)?),\s*(-?\d+(\.\d+)?)')
                               
                for line in file:
                    # Try to match the pattern in the current line
                    match = pattern.match(line)
                    if match:
                        min = match.group(1)
                        max = match.group(3)                
                        if float(min) < sensor_limits[item['value']]["min"] or float(max) > sensor_limits[item['value']]["max"]:
                                print(f"ERROR: Sensor {item['value']} {min} : {max} not within min and max range {sensor_limits[item['value']]}")
                                buffer.write(f"<strong>ERROR: Sensor {item['value']} {min} : {max} not within min and max range {sensor_limits[item['value']]}</strong><br>")
                                errors_found = True
                        found = True        
                        break
                        
                if not found:
                    print(f"Sensor {item['value']} span range not found")
                    buffer.write(f"Sensor {item['value']} span range not found<br>")
                    errors_found = True
                                        

def review_data(hdr_file_path):
    global errors_found
    summary_buffer = StringIO()
    sensor_list = ["prDM", "t090C", "t190C", "c0S/m", "c1S/m", "CStarAt0", "CStarTr0", "flECO-AFL", "pumps", "latitude", "longitude", "sal00", "sal11", "sbeox0V", "sbeox1VL"]
    
    print(f"Checking Processed Header files for sensors and their ranges")
    summary_buffer.write(f"<strong>Checking Processed Header files for sensors and their ranges</strong><br>")    
    print(f"Checking for expected sensor variables: {sensor_list}")
    summary_buffer.write(f"<br>Checking for expected sensor variables: {sensor_list}<br><br>")

    files = find_hdr_files(hdr_file_path)
    if len(files) == 0:
        print(f"There are no header files to check for this cruise.")
        summary_buffer.write(f"There are no header files to check for this cruise.<br>")
        errors_found = True
    
    for file in files:
        print(f"Checking {file}")
        buffer.write(f"<strong><br><br>Checking {file}</strong><br>")
        # Check NMEA Lat, Long
        check_nmea(file)
        # Check for any duplicate variable names, not just the ones in the sensor_list
        check_duplicates(file)
        # Check that sensors are included in .hdr files and perform range checks
        check_sensor_data(file)
        # Check for modulo error
        check_modulo(file)

   # remove duplicate errors in buffer
    error_files = summarize_errors()

    found_in_all = False
    for error in error_files:
        if len(files) == len(error_files[error]):
            if not found_in_all:
                summary_buffer.write(f"<u>Found in all header files:</u> <br>")
            summary_buffer.write(f"{error}")
            found_in_all = True
        else:
            summary_buffer.write(f"<br><u>Errors found in files:</u> {', <br>'.join(error_files[error])}<br>")  #carriage return between filenames
            summary_buffer.write(f"{error}")
            found_in_all = False
        summary_buffer.write(f"<br>")
        
    if not errors_found:
        print(f"NO ERRORS FOUND.")
        summary_buffer.write(f"<br><br><strong>NO ERRORS FOUND.</strong><br>")
    else:
        print(f"ERRORS FOUND!")
        summary_buffer.write(f"<br><br><strong>ERRORS FOUND!</strong><br>")
        
            
    match = re.search(r'ship-provided_data_(.*?)\\', hdr_file_path)
    if match:
        cruise_name = match.group(1)
    else:
        print(f"Cruise name pattern not found in file path.")
        summary_buffer.write(f"Cruise name pattern not found in file path.<br>")

    buffer_content = summary_buffer.getvalue()
    summary_buffer.close()
    with open(current_dir + "/" + cruise_name +"_ctd_hdr_review_results.html", "w") as file:   # txt files don't support bold font
        file.write(buffer_content)
    

def main():
    # If paths are on Google Docs, you must download the files to your local pc file system.
    parser = argparse.ArgumentParser(description='Review CTD header data prior to upload to RDS for NES-LTER REST API.')
    parser.add_argument('path', type=str, help='Path to CTD processed header directory')
    
    args = parser.parse_args()
    
    review_data(args.path)

if __name__ == '__main__':
    main()
